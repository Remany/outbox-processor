package ru.romanov.outbox.storage;

import ru.romanov.outbox.domain.model.OutboxMessageEntity;

public interface OutboxMessageQueue {
    void add(OutboxMessageEntity outboxMessageEntity);
    OutboxMessageEntity take();
}
