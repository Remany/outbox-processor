package ru.romanov.outbox.service;

import lombok.RequiredArgsConstructor;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;
import ru.romanov.outbox.storage.OutboxMessageQueue;
import ru.romanov.outbox.storage.UpdateEntry;

import java.time.LocalDateTime;

@RequiredArgsConstructor
public abstract class AbstractOutboxRecoveryService {

    private final StreamingProperties properties;
    private final OutboxMessageRepository repository;
    private final OutboxMessageQueue queue;

    public abstract void recoverStuckMessages();

    /**
     * Застрявшим сообщением считается сообщение в статусе NEW или FAILURE и reservedTo < now()
     * Обновляем reservedTo на константное время, чтобы неявно поставить лок на него
     * Сохраняем измененную сущность и после коммита транзакции помещаем сообщение в очередь воркеров
     */
    public void process() {
        if (!properties.getOutbox().isStreamingEnabled()) {
            return;
        }
        final var recovery = properties.getOutbox().getRecovery();
        final var batch = repository.findStuckMessages(recovery.getLimit()).stream().peek(message -> {
            message.setReservedTo(LocalDateTime.now().plusSeconds(recovery.getAdditionalReserveTime()));
            message.setStatus(OutboxMessageStatus.NEW);
            queue.addAfterTxCommit(message);
        }).map(message -> UpdateEntry.of(message.getId(), message.getStatus(), message.getReservedTo())).toList();
        repository.updateStatuses(batch);
    }
}
