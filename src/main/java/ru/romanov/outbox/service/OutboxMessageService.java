package ru.romanov.outbox.service;

import java.util.Map;

public interface OutboxMessageService {
    void save(String key, Object data, Map<String, String> platformToTopic, Map<String, String> headers);
}
