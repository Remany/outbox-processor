package ru.romanov.outbox.scheduler.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;
import ru.romanov.outbox.scheduler.OutboxCleanJob;

@Slf4j
@RequiredArgsConstructor
public class OutboxCleanJobImpl implements OutboxCleanJob {

    private final OutboxMessageRepository repository;
    private final StreamingProperties properties;

    @Override
    @Transactional
    @Scheduled(cron = "${streaming.outbox.cleanup.cron:* * 21 * * *}")
    public void cleanupProcessedMessages() {
        log.info("Производим ежедневную чистку отправленных outbox сообщений");
        final var cleanup = properties.getOutbox().getCleanup();
        repository.deleteProcessedMessages(
                cleanup.getDaysInterval(), cleanup.getLimit());
        log.info("Ежедневная чистка outbox сообщений выполнена успешно");
    }
}
