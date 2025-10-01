package ru.romanov.outbox.streaming;

import java.util.Map;

public interface StreamingDataProcessor {
    void initDataProcessing(String key, Object payload, Map<String, String> platformsToTopic);

    void initDataProcessing(String key, Object payload, Map<String, String> platformsToTopic,
                            Map<String, String> headers);
}
