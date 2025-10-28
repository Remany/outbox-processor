package ru.romanov.outbox.processing.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.configuration.kafka.KafkaTemplateFactory;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;
import ru.romanov.outbox.metric.OutboxMetrics;
import ru.romanov.outbox.processing.AbstractOutboxProcessor;
import ru.romanov.outbox.service.OutboxMessageStatusService;
import ru.romanov.outbox.storage.MessageRef;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.Executor;

@Slf4j
public class ScheduledOutboxProcessor extends AbstractOutboxProcessor {

    private final KafkaTemplateFactory kafkaTemplateFactory;
    private final Executor messagesExecutor;
    private final StreamingProperties properties;
    private final OutboxMetrics outboxMetrics;
    private final OutboxMessageStatusService statusService;
    private final OutboxMessageRepository repository;

    public ScheduledOutboxProcessor(ObjectMapper objectMapper,
                                    OutboxMessageStatusService statusService,
                                    KafkaTemplateFactory kafkaTemplateFactory,
                                    Executor messagesExecutor, StreamingProperties properties,
                                    OutboxMetrics outboxMetrics, OutboxMessageRepository repository) {
        super(objectMapper, statusService);
        this.kafkaTemplateFactory = kafkaTemplateFactory;
        this.messagesExecutor = messagesExecutor;
        this.properties = properties;
        this.outboxMetrics = outboxMetrics;
        this.statusService = statusService;
        this.repository = repository;
    }

    @Override
    @Transactional
    @Scheduled(
            initialDelay = 10000,
            fixedDelayString = "${streaming.outbox.scheduled.delay}")
    public void processingMessages() {
        final var limit = properties.getOutbox().getScheduled().getLimit();
        repository.findNewMessages(limit).forEach(this::send);
    }

    private void send(OutboxMessageEntity message) {
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
    }
}
