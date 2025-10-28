package ru.romanov.outbox.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class OutboxMetrics {

    private final MeterRegistry registry;

    private final Counter eventsStoredTotal;
    private final Counter publicationAttemptsTotal;
    private final Counter publicationSuccessTotal;
    private final Counter publicationErrorsTotal;

    private final Timer publicationLatency;
    private final Timer storedToAttemptLatency;

    private final AtomicInteger pendingEventsGauge = new AtomicInteger(0);

    public OutboxMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.eventsStoredTotal = Counter.builder("outbox_events_stored_total")
                .description("Total number of events stored in the outbox table")
                .register(registry);

        this.publicationAttemptsTotal = Counter.builder("outbox_kafka_publication_attempts_total")
                .description("Total Kafka publication attempts")
                .register(registry);

        this.publicationSuccessTotal = Counter.builder("outbox_kafka_publication_success_total")
                .description("Total successfully published events to Kafka")
                .register(registry);

        this.publicationErrorsTotal = Counter.builder("outbox_kafka_publication_errors_total")
                .description("Total publication errors to Kafka")
                .register(registry);

        this.publicationLatency = Timer.builder("outbox_kafka_publication_latency_seconds")
                .description("Kafka publication latency in seconds")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry);

        this.storedToAttemptLatency = Timer.builder("outbox_stored_to_attempt_latency_seconds")
                .description("Latency from storing event to attempt Kafka publication")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry);

        Gauge.builder("outbox_events_pending", pendingEventsGauge, AtomicInteger::get)
                .description("Number of pending outbox events waiting for publication")
                .register(registry);
    }

    public void messageStored() {
        eventsStoredTotal.increment();
        pendingEventsGauge.incrementAndGet();
    }

    public void messagePublicationAttempted(String system, String topic) {
        publicationAttemptsTotal.increment();

        Counter.builder("outbox_kafka_publication_detailed_total")
                .tag("system", safe(system))
                .tag("topic", safe(topic))
                .tag("status", "attempted")
                .register(registry)
                .increment();
    }

    public void messagePublicationSucceeded(String system, String topic, long durationMillis) {
        publicationSuccessTotal.increment();
        pendingEventsGauge.decrementAndGet();

        publicationLatency.record(durationMillis, TimeUnit.MILLISECONDS);

        Counter.builder("outbox_kafka_publication_detailed_total")
                .tag("system", safe(system))
                .tag("topic", safe(topic))
                .tag("status", "success")
                .register(registry)
                .increment();
    }

    public void messagePublicationFailed(String system, String topic, String errorType) {
        publicationErrorsTotal.increment();
        pendingEventsGauge.decrementAndGet();

        Counter.builder("outbox_kafka_publication_detailed_total")
                .tag("system", safe(system))
                .tag("topic", safe(topic))
                .tag("status", "error")
                .tag("errorType", safe(errorType))
                .register(registry)
                .increment();
    }

    public void recordStoredToAttemptedLatency(long createdTimeMillis) {
        long now = System.currentTimeMillis();
        long duration = now - createdTimeMillis;
        storedToAttemptLatency.record(duration, TimeUnit.MILLISECONDS);
    }

    private String safe(String tagValue) {
        return tagValue == null ? "unknown" : tagValue;
    }
}
