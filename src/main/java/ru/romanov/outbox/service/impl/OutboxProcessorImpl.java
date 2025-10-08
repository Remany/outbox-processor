package ru.romanov.outbox.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.romanov.outbox.service.OutboxProcessor;
import ru.romanov.outbox.storage.OutboxMessageQueue;

@Component
@RequiredArgsConstructor
public class OutboxProcessorImpl implements OutboxProcessor {

    private final OutboxMessageQueue messageQueue;

    @Override
    public void startProcessingMessages() {

    }
}
