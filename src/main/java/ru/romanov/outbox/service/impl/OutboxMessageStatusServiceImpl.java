package ru.romanov.outbox.service.impl;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;
import ru.romanov.outbox.service.OutboxMessageStatusService;
import ru.romanov.outbox.storage.MessageRef;
import ru.romanov.outbox.storage.UpdateEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OutboxMessageStatusServiceImpl implements OutboxMessageStatusService {

    private final BlockingQueue<UpdateEntry> queue;
    private final ScheduledExecutorService scheduler;
    private final OutboxMessageRepository outboxRepository;
    private final StreamingProperties.OutboxProperties.StatusUpdate properties;
    private final Executor messagesExecutor;

    private final Counter successfulFlushes;
    private final Counter failedFlushes;
    private final Gauge queueSizeGauge;
    private final Timer flushTimer;

    public OutboxMessageStatusServiceImpl(OutboxMessageRepository outboxRepository, StreamingProperties properties,
                                          MeterRegistry meterRegistry, Executor messagesExecutor) {
        this.outboxRepository = outboxRepository;
        this.messagesExecutor = messagesExecutor;
        this.properties = properties.getOutbox().getStatusUpdate();

        this.queue = new LinkedBlockingQueue<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        this.successfulFlushes = Counter.builder("outbox_status_updater_flush_success_total")
                .description("Number of successful status batch updates")
                .register(meterRegistry);
        this.failedFlushes = Counter.builder("outbox_status_updater_flush_fail_total")
                .description("The number of failed batch status updates")
                .register(meterRegistry);
        this.flushTimer = Timer.builder("outbox_status_updater_flush_latency_seconds")
                .description("The time of the status update batch")
                .register(meterRegistry);
        this.queueSizeGauge = Gauge.builder("outbox_status_updater_queue_size", queue, Collection::size)
                .description("Current size of the status update queue")
                .register(meterRegistry);

        scheduler.scheduleAtFixedRate(this::flushIfNeeded, this.properties.getFlushIntervalMs(), this.properties.getFlushIntervalMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void addSuccess(MessageRef ref) {
        add(ref, OutboxMessageStatus.SUCCESS);
    }

    @Override
    public void addFailure(MessageRef ref) {
        add(ref, OutboxMessageStatus.FAILURE);
    }

    private void add(MessageRef ref, OutboxMessageStatus status) {
        if (queue.size() >= properties.getMaxQueueSize()) {
            log.warn("Очередь обновления статусов переполнена: [{}] элементов, сообщение [{}] отбрасывается", queue.size(), ref.id());
            return;
        }
        queue.add(UpdateEntry.of(ref, status));
        if (queue.size() >= properties.getBatchSize()) {
            flushAsync();
        }
    }

    private void flushIfNeeded() {
        if (!queue.isEmpty()) {
            flushAsync();
        }
    }

    private void flushAsync() {
        List<UpdateEntry> batch = new ArrayList<>(properties.getBatchSize());
        queue.drainTo(batch, properties.getBatchSize());
        if (batch.isEmpty()) {
            return;
        }
        messagesExecutor.execute(() -> flushTimer.record(() -> flushWithRetry(batch)));
    }

    private void flushWithRetry(List<UpdateEntry> batch) {
        int attempts = 0;
        while (attempts < properties.getMaxRetries()) {
            try {
                outboxRepository.updateStatuses(batch);
                successfulFlushes.increment();
                return;
            } catch (Exception e) {
                attempts++;
                log.warn("Ошибка при обновлении статусов (попытка [{}] из [{}]) — [{}]", attempts, properties.getMaxRetries(), e.getMessage());
                if (attempts < properties.getMaxRetries()) {
                    try {
                        Thread.sleep(properties.getRetryDelayMs());
                    } catch (InterruptedException ignored) {
                    }
                } else {
                    failedFlushes.increment();
                    queue.addAll(batch);
                    log.error("Все попытки обновления исчерпаны, batch возвращён в очередь", e);
                }
            }
        }
    }

    @Override
    public void shutdown() {
        scheduler.shutdownNow();
    }
}
