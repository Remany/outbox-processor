package ru.romanov.outbox.storage;

import java.util.UUID;

public record MessageRef(UUID id, String kafkaSystem, String topic, String key) {

    public static MessageRef of(UUID id) {
        return new MessageRef(id, null, null, null);
    }

    public static MessageRef of(UUID id, String kafkaSystem, String topic, String key) {
        return new MessageRef(id, kafkaSystem, topic, key);
    }
}

