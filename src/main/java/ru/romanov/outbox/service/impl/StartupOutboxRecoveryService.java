package ru.romanov.outbox.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.transaction.annotation.Transactional;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;
import ru.romanov.outbox.service.OutboxRecoveryService;
import ru.romanov.outbox.storage.OutboxMessageQueue;

import java.time.LocalDateTime;

@RequiredArgsConstructor
public class StartupOutboxRecoveryService implements OutboxRecoveryService {

    private static final int COUNT_OF_STUCK_MESSAGES = 1000;
    private static final int ADDITIONAL_RESERVE_TIME = 30;

    private final OutboxMessageRepository repository;
    private final OutboxMessageQueue queue;
    private final StreamingProperties properties;

    /* Запрашиваем на старте приложения застрявшие сообщения */
    @Override
    @Transactional
    @EventListener(ApplicationReadyEvent.class)
    public void recoverStuckMessages() {
        if (!properties.getOutbox().isStreamingEnabled()) {
            return;
        }
        /* Застрявшим сообщением считается сообщение в статусе NEW или FAILURE и reservedTo < now() */
        /* Обновляем reservedTo на константное время, чтобы неявно поставить лок на него */
        /* Сохраняем измененную сущность и после коммита транзакции помещаем сообщение в очередь воркеров  */
        repository.findStuckMessages(COUNT_OF_STUCK_MESSAGES)
                .stream()
                .map(message -> withReservedTo(message, LocalDateTime.now()
                        .plusSeconds(ADDITIONAL_RESERVE_TIME)))
                .forEach(updated -> {
                    repository.updateState(updated);
                    queue.addAfterTxCommit(updated);
                });
    }

    /* Обновляем status, reservedTo */
    private OutboxMessageEntity withReservedTo(OutboxMessageEntity original, LocalDateTime reservedTo) {
        return OutboxMessageEntity.builder()
                .id(original.getId())
                .key(original.getKey())
                .payload(original.getPayload())
                .headers(original.getHeaders())
                .topic(original.getTopic())
                .kafkaSystem(original.getKafkaSystem())
                .createTime(original.getCreateTime())
                .reservedTo(reservedTo)
                .status(OutboxMessageStatus.NEW)
                .build();
    }
}
