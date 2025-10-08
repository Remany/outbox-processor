package ru.romanov.outbox.service;

public interface OutboxRecoveryService {
    void recoverStuckMessages();
}
