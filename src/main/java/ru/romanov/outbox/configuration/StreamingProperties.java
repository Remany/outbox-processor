package ru.romanov.outbox.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "streaming")
public class StreamingProperties {

    private OutboxProperties outbox = new OutboxProperties();

    @Getter
    @Setter
    public static class OutboxProperties {

        private static final Integer DEFAULT_CONCURRENCY = Runtime.getRuntime().availableProcessors();

        private boolean enabled = true;

        private boolean streamingEnabled = true;

        private String tableName;

        private String sourceId;

        private Integer concurrency = DEFAULT_CONCURRENCY;
    }
}
