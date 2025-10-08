package ru.romanov.outbox.facade;

import java.util.Map;

public interface StreamingFacade {
    /**
     * @param key             ключ сообщения
     * @param payload         тело сообщения
     * @param platformToTopic направление сообщения
     */
    void initDataProcessing(String key, Object payload, Map<String, String> platformToTopic);

    /**
     * @param key             ключ сообщения
     * @param payload         тело сообщения
     * @param platformToTopic направление сообщения
     * @param headers         заголовки сообщения
     */
    void initDataProcessing(String key, Object payload, Map<String, String> platformToTopic, Map<String, String> headers);
}
