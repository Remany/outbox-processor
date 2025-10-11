package ru.romanov.outbox.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Getter
@Setter
@ConfigurationProperties(value = "streaming")
public class StreamingProperties {

    private boolean enabled = true;

    private OutboxProperties outbox = new OutboxProperties();

    private DynamicKafka kafka = new DynamicKafka();

    @Getter
    @Setter
    public static class OutboxProperties {

        private boolean enabled = true;

        private boolean streamingEnabled = true;

        private String tableName;

        private String sourceId;

        private Integer concurrency = Runtime.getRuntime().availableProcessors();

        private Cleanup cleanup = new Cleanup();

        @Getter
        @Setter
        public static class Cleanup {
            private String cron;
            private int daysInterval = 7;
            private int limit = 1000;
        }
    }

    @Getter
    @Setter
    public static class DynamicKafka {

        private String securityProtocol;

        /**
         * Уникальные параметры для каждого kafka producer
         */
        private Map<String, KafkaSystemProperties> systems;

        private Producer producer = new Producer();

        @Getter
        @Setter
        public static class KafkaSystemProperties {
            private String bootstrapServers;

            private Ssl ssl;
        }

        @Getter
        @Setter
        public static class Ssl {
            private Ssl.TrustStore trustStore;

            private Ssl.KeyStore keyStore;

            private Ssl.Key key;

            private String protocol = "TLS";

            private boolean enabled;

            @Getter
            @Setter
            public static class TrustStore {
                private String location;

                private String password;

                private String type;
            }

            @Getter
            @Setter
            public static class KeyStore {
                private String location;

                private String password;

                private String type;
            }

            @Getter
            @Setter
            public static class Key {
                private String password;
            }
        }

        @Getter
        @Setter
        public static class Producer {
            private static final Integer DEFAULT_BATCH_SIZE = 1_000_000;

            private static final Integer DEFAULT_TIMEOUT_MS = 300000;

            private Integer batchSize = DEFAULT_BATCH_SIZE;

            private Integer deliveryTimeoutMs = DEFAULT_TIMEOUT_MS;
        }
    }
}
