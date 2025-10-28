package ru.romanov.outbox.processing.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.configuration.kafka.KafkaTemplateFactory;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.metric.OutboxMetrics;
import ru.romanov.outbox.processing.AbstractOutboxProcessor;
import ru.romanov.outbox.service.OutboxMessageStatusService;
import ru.romanov.outbox.storage.MessageRef;
import ru.romanov.outbox.storage.OutboxMessageQueue;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.Executor;

@Slf4j
public class EventOrientedOutboxProcessorImpl extends AbstractOutboxProcessor {

    private final OutboxMessageQueue queue;
    private final KafkaTemplateFactory kafkaTemplateFactory;
    private final Executor messagesExecutor;
    private final StreamingProperties properties;
    private final OutboxMetrics outboxMetrics;

    public EventOrientedOutboxProcessorImpl(OutboxMessageQueue queue, ObjectMapper objectMapper,
                                            OutboxMessageStatusService statusService,
                                            KafkaTemplateFactory kafkaTemplateFactory,
                                            Executor messagesExecutor, StreamingProperties properties,
                                            OutboxMetrics outboxMetrics) {
        super(objectMapper, statusService);
        this.queue = queue;
        this.kafkaTemplateFactory = kafkaTemplateFactory;
        this.messagesExecutor = messagesExecutor;
        this.properties = properties;
        this.outboxMetrics = outboxMetrics;
    }

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
                OutboxMessageEntity message = queue.take();
                final UUID id = message.getId();
                final String key = message.getKey();
                final String kafkaSystem = message.getKafkaSystem();
                final String topic = message.getTopic();
                final LocalDateTime createTime = message.getCreateTime();

                KafkaTemplate<String, String> producer = kafkaTemplateFactory.resolveKafkaTemplate(kafkaSystem);

                ProducerRecord<String, String> record = createRecord(message);

                outboxMetrics.messagePublicationAttempted(kafkaSystem, topic);

                final long createdAtMillis = createTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                outboxMetrics.recordStoredToAttemptedLatency(createdAtMillis);

                long startTime = System.currentTimeMillis();

                producer.send(record).whenCompleteAsync((result, ex) -> {
                    long duration = System.currentTimeMillis() - startTime;

                    if (ex != null) {
                        handleErrorRef(MessageRef.of(id, kafkaSystem, topic, key), ex);
                        outboxMetrics.messagePublicationFailed(kafkaSystem, topic, classifyError(ex));
                        return;
                    }

                    log.info("Успешно отправлено сообщение в Kafka, система потребитель: [{}], topic: [{}], key: [{}]",
                            kafkaSystem, topic, key);
                    statusService.addSuccess(new MessageRef(id, kafkaSystem, topic, key));
                    outboxMetrics.messagePublicationSucceeded(kafkaSystem, topic, duration);
                }, messagesExecutor);

            } catch (InterruptedException e) {
                logErr(e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }
}
