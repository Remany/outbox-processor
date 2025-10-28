package ru.romanov.outbox.service;

import ru.romanov.outbox.storage.MessageRef;

public interface OutboxMessageStatusService {

    void addSuccess(MessageRef ref);

    void addFailure(MessageRef ref);

    void shutdown();
}
