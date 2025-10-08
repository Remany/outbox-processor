package ru.romanov.outbox.facade.impl;

import lombok.extern.slf4j.Slf4j;
import ru.romanov.outbox.service.OutboxMessageService;

import java.util.Map;

@Slf4j
public class OutboxStreamingFacade extends AbstractStreamingFacade {

    public OutboxStreamingFacade(OutboxMessageService outboxService) {
        super(outboxService);
    }

    @Override
    protected void process(String key, Object data, Map<String, String> platformToTopic, Map<String, String> headers) {
        outboxService.save(key, data, platformToTopic, headers);
    }
}
