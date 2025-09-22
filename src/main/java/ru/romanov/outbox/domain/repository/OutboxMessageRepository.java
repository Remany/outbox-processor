package ru.romanov.outbox.domain.repository;

import ru.romanov.outbox.domain.model.OutboxMessageEntity;

import java.util.Set;

public interface OutboxMessageRepository {
    OutboxMessageEntity save(OutboxMessageEntity outboxMessageEntity);

    Set<OutboxMessageEntity> findStuckMessagesEntity(int limit);
}
