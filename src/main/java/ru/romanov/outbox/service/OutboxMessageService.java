package ru.romanov.outbox.service;

import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;

import java.util.Map;

public interface OutboxMessageService {
    void save(String key, Object data, Map<String, String> platformToTopic, Map<String, String> headers);

    void updateStatus(OutboxMessageEntity outboxMessage, OutboxMessageStatus status);
}
