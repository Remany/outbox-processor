package ru.romanov.outbox.domain.repository;

import ru.romanov.outbox.domain.model.OutboxMessageEntity;

import java.util.List;

public interface OutboxMessageRepository {
    OutboxMessageEntity save(OutboxMessageEntity outboxMessageEntity);

    void updateState(OutboxMessageEntity outboxMessageEntity);

    List<OutboxMessageEntity> findStuckMessages(Integer limit);

    void deleteProcessedMessages(Integer retentionDate, Integer limit);
}
