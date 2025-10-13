package ru.romanov.outbox.service.impl;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;
import ru.romanov.outbox.service.OutboxRecoveryService;
import ru.romanov.outbox.storage.OutboxMessageQueue;

public class ScheduledOutboxRecoveryService extends OutboxRecoveryService {

    public ScheduledOutboxRecoveryService(StreamingProperties properties,
                                          OutboxMessageRepository repository,
                                          OutboxMessageQueue queue) {
        super(properties, repository, queue);
    }

    /* Запрашиваем на старте приложения застрявшие сообщения */
    @Override
    @Transactional
    @Scheduled(
            cron = "${streaming.outbox.recovery.cron:*/30 * * * * *}",
            fixedDelayString = "${streaming.outbox.recovery.cron.initial-delay:60000}")
    public void recoverStuckMessages() {
        process();
    }
}
