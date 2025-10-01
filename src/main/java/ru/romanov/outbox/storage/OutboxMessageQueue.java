package ru.romanov.outbox.storage;

import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;

public interface OutboxMessageQueue {
    void add(OutboxMessageEntity message);

    void addAfterTxCommit(OutboxMessageEntity message);

    OutboxMessageEntity take(OutboxMessageStatus status) throws InterruptedException;

    void markSuccess(OutboxMessageEntity message);

    void markFailure(OutboxMessageEntity message);
}
