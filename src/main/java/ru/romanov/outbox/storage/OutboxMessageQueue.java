package ru.romanov.outbox.storage;

import ru.romanov.outbox.domain.model.OutboxMessageEntity;

public interface OutboxMessageQueue {
    void add(OutboxMessageEntity message);

    void addAfterTxCommit(OutboxMessageEntity message);

    OutboxMessageEntity take() throws InterruptedException;
}
