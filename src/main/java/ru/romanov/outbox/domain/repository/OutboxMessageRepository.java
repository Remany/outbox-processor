package ru.romanov.outbox.domain.repository;

import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.storage.UpdateEntry;

import java.util.List;

public interface OutboxMessageRepository {
    OutboxMessageEntity save(OutboxMessageEntity outboxMessageEntity);

    void updateStatuses(List<UpdateEntry> batch);

    List<OutboxMessageEntity> findStuckMessages(Integer limit);

    void deleteProcessedMessages(Integer retentionDate, Integer limit);
}
