package ru.romanov.outbox.processing.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.configuration.kafka.KafkaTemplateFactory;
import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.metric.OutboxMetrics;
import ru.romanov.outbox.processing.OutboxProcessor;
import ru.romanov.outbox.service.OutboxMessageStatusService;
import ru.romanov.outbox.storage.MessageRef;
import ru.romanov.outbox.storage.OutboxMessageQueue;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

@Slf4j
@RequiredArgsConstructor
public class EventOrientedOutboxProcessorImpl implements OutboxProcessor {

    private final OutboxMessageQueue queue;
    private final KafkaTemplateFactory kafkaTemplateFactory;
    private final Executor messagesExecutor;
    private final StreamingProperties properties;
    private final ObjectMapper objectMapper;
    private final OutboxMetrics outboxMetrics;
    private final OutboxMessageStatusService statusService;

    private static final TypeReference<Map<String, String>> HEADER_TYPE = new TypeReference<>() {
    };

    @PostConstruct
    public void processingMessages() {
        final int concurrency = properties.getOutbox().getConcurrency();
        for (int i = 0; i < concurrency / 2; i++) {
            messagesExecutor.execute(this::processNewMessages);
        }
    }

    private void processNewMessages() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                OutboxMessageEntity message = queue.take(OutboxMessageStatus.NEW);
                final UUID id = message.getId();
                final String key = message.getKey();
                final String kafkaSystem = message.getKafkaSystem();
                final String topic = message.getTopic();

                KafkaTemplate<String, String> producer = kafkaTemplateFactory.resolveKafkaTemplate(kafkaSystem);

                ProducerRecord<String, String> record = createRecord(message);

                outboxMetrics.messagePublicationAttempted(kafkaSystem, topic);
                long startTime = System.currentTimeMillis();

                producer.send(record).whenCompleteAsync((result, ex) -> {
                    long duration = System.currentTimeMillis() - startTime;

                    if (ex != null) {
                        handleErrorRef(MessageRef.of(id, kafkaSystem, topic, key), ex);
                        outboxMetrics.messagePublicationFailed(kafkaSystem, topic, classifyError(ex));
                        return;
                    }

                    log.info("Успешно отправлено сообщение в Kafka, система потребитель: [{}], topic: [{}], key: [{}]", kafkaSystem, topic, key);
                    statusService.addSuccess(new MessageRef(id, kafkaSystem, topic, key));
                    outboxMetrics.messagePublicationSucceeded(kafkaSystem, topic, duration);
                }, messagesExecutor);

            } catch (InterruptedException e) {
                logErr(e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    private void handleErrorRef(MessageRef ref, Throwable ex) {
        log.error("Не получилось отправить сообщение в Kafka, система потребитель: [{}], topic: [{}], key: [{}], подробности: [{}]",
                ref.kafkaSystem(), ref.topic(), ref.key(), ex.getMessage());
        statusService.addFailure(ref);
    }

    private ProducerRecord<String, String> createRecord(OutboxMessageEntity message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(message.getTopic(), message.getKey(), message.getPayload());
        if (message.getHeaders() != null) {
            var _headers = parseHeaders(message.getHeaders());
            _headers.forEach((k, v) -> record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))));
        }
        return record;
    }

    private Map<String, String> parseHeaders(String headers) {
        try {
            return objectMapper.readValue(headers, HEADER_TYPE);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error while trying to parse headers", e);
        }
    }

    private String classifyError(Throwable ex) {
        if (ex == null) return "unknown";

        String className = ex.getClass().getSimpleName().toLowerCase();

        if (className.contains("timeout")) return "timeout";
        if (className.contains("network")) return "network_error";
        if (className.contains("serialization")) return "serialization_error";
        if (className.contains("authorization")) return "auth_error";
        if (className.contains("authentication")) return "auth_error";
        if (className.contains("coordinator")) return "coordinator_error";

        return "other";
    }

    private void logErr(String details) {
        log.error("Ошибка в работе outbox worker, подробности: [{}]", details);
    }
}
