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
import ru.romanov.outbox.processing.OutboxProcessor;
import ru.romanov.outbox.service.OutboxMessageService;
import ru.romanov.outbox.storage.OutboxMessageQueue;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executor;

@Slf4j
@RequiredArgsConstructor
public class OutboxProcessorImpl implements OutboxProcessor {

    private final OutboxMessageQueue queue;
    private final KafkaTemplateFactory kafkaTemplateFactory;
    private final Executor messagesExecutor;
    private final OutboxMessageService outboxService;
    private final StreamingProperties properties;
    private final ObjectMapper objectMapper;

    @PostConstruct
    public void startProcessingMessages() {
        final int concurrency = properties.getOutbox().getConcurrency();
        submitWithConcurrency(this::processNewMessages, concurrency);
        submitWithConcurrency(this::processSuccessMessages, concurrency);
        submitWithConcurrency(this::processFailureMessages, concurrency);
    }

    private void submitWithConcurrency(Runnable task, int concurrency) {
        for (int i = 0; i < concurrency / 3; i++) {
            messagesExecutor.execute(task);
        }
    }

    private void processNewMessages() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                OutboxMessageEntity message = queue.take(OutboxMessageStatus.NEW);
                String key = message.getKey();
                String kafkaSystem = message.getKafkaSystem();
                String topic = message.getTopic();

                KafkaTemplate<String, String> producer =
                        kafkaTemplateFactory.resolveKafkaTemplate(kafkaSystem);

                ProducerRecord<String, String> record = createRecord(message);

                try {
                    producer
                            .send(record)
                            .whenComplete(
                                    (result, ex) -> {
                                        if (ex != null) {
                                            handleError(message, ex);
                                            return;
                                        }
                                        log.info(
                                                "Успешно отправлено сообщение в Kafka, система потребитель: [{}], topic: [{}], key: [{}]",
                                                kafkaSystem,
                                                topic,
                                                key);
                                        queue.markSuccess(message);
                                    });
                } catch (Exception e) {
                    handleError(message, e);
                }

            } catch (InterruptedException e) {
                logErr(e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    private void handleError(OutboxMessageEntity message, Throwable ex) {
        log.error(
                "Не получилось отправить сообщение в Kafka, система потребитель: [{}], topic: [{}], key: [{}], подробности: [{}]",
                message.getKafkaSystem(),
                message.getTopic(),
                message.getKey(),
                ex.getMessage());
        queue.markFailure(message);
    }

    private void processSuccessMessages() {
        while (!Thread.currentThread().isInterrupted()) {
            updateStatus(OutboxMessageStatus.SUCCESS);
        }
    }

    private void processFailureMessages() {
        while (!Thread.currentThread().isInterrupted()) {
            updateStatus(OutboxMessageStatus.FAILURE);
        }
    }

    private void updateStatus(OutboxMessageStatus status) {
        try {
            var successMessage = queue.take(status);
            outboxService.updateStatus(successMessage, status);
        } catch (InterruptedException e) {
            logErr(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void logErr(String details) {
        log.error("Ошибка в работе outbox worker, подробности: [{}]", details);
    }

    private ProducerRecord<String, String> createRecord(OutboxMessageEntity message) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(
                        message.getTopic(), message.getKey(), message.getPayload().asText());
        if (message.getHeaders() != null) {
            var _headers = parseHeaders(message.getHeaders().asText());
            _headers.forEach(
                    (k, v) -> record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))));
        }
        return record;
    }

    private Map<String, String> parseHeaders(String headers) {
        try {
            return objectMapper.readValue(headers, new TypeReference<>() {
            });
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error while trying to parse headers", e);
        }
    }
}
