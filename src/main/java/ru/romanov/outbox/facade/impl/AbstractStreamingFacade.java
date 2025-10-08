package ru.romanov.outbox.facade.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.romanov.outbox.facade.StreamingFacade;
import ru.romanov.outbox.service.OutboxMessageService;

import java.util.Collections;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractStreamingFacade implements StreamingFacade {

    protected final OutboxMessageService outboxService;

    @Override
    public void initDataProcessing(String modelId, Object data, Map<String, String> platformToTopic) {
        final Map<String, String> emptyHeaders = Collections.emptyMap();
        process(modelId, data, platformToTopic, emptyHeaders);
    }

    @Override
    public void initDataProcessing(String modelId, Object data, Map<String, String> platformToTopic, Map<String, String> headers) {
        process(modelId, data, platformToTopic, headers);
    }

    /**
     * Обработка сообщения перед сохранением в таблицу outbox. Добавление хедеров и мета полей.
     *
     * @param key     ключ сообщения
     * @param data    данные входящей сущности
     * @param headers заголовки
     */
    protected abstract void process(String key, Object data, Map<String, String> platformToTopic, Map<String, String> headers);
}
