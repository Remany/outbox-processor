package ru.romanov.outbox.scheduler;

public interface OutboxCleanJob {
    void cleanupProcessedMessages();
}
