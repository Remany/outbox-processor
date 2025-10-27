package ru.romanov.outbox.service.impl;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.transaction.annotation.Transactional;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;
import ru.romanov.outbox.service.AbstractOutboxRecoveryService;
import ru.romanov.outbox.storage.OutboxMessageQueue;

public class StartupOutboxRecoveryService extends AbstractOutboxRecoveryService {

    public StartupOutboxRecoveryService(StreamingProperties properties,
                                        OutboxMessageRepository repository,
                                        OutboxMessageQueue queue) {
        super(properties, repository, queue);
    }

    /* Запрашиваем на старте приложения застрявшие сообщения */
    @Override
    @Transactional
    @EventListener(ApplicationReadyEvent.class)
    public void recoverStuckMessages() {
        process();
    }
}
