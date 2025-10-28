package ru.romanov.outbox.configuration.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import ru.romanov.outbox.configuration.StreamingProperties;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class DynamicKafkaProducerConfiguration {

    private final StreamingProperties properties;
    private final MeterRegistry meterRegistry;

    @Bean
    public Map<String, KafkaTemplate<String, String>> kafkaTemplates() {
        Map<String, KafkaTemplate<String, String>> templates = new HashMap<>();
        if (properties.getKafka().getSystems() != null) {
            for (String kafkaSystem : properties.getKafka().getSystems().keySet()) {
                templates.put(kafkaSystem, createKafkaTemplate(kafkaSystem, meterRegistry));
            }
        } else {
            log.warn("No configured kafka systems found");
        }

        return templates;
    }

    @Bean
    public KafkaTemplateFactory kafkaTemplateFactory(@Qualifier("kafkaTemplates") Map<String, KafkaTemplate<String, String>> kafkaTemplates) {
        return new KafkaTemplateFactory(kafkaTemplates);
    }

    private KafkaTemplate<String, String> createKafkaTemplate(String kafkaSystem, MeterRegistry meterRegistry) {
        Map<String, Object> combinedProps = new HashMap<>(getCommonProperties());
        combinedProps.putAll(getSystemProperties(kafkaSystem));
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(combinedProps);

        try (Producer<String, String> probeProducer = producerFactory.createProducer()) {
            new KafkaClientMetrics(probeProducer).bindTo(meterRegistry);
        }

        return new KafkaTemplate<>(producerFactory);
    }

    private Map<String, Object> getSystemProperties(String kafkaSystem) {
        Map<String, Object> systemProps = new HashMap<>();

        if (properties.getKafka().getSystems() != null && !properties.getKafka()
                .getSystems()
                .containsKey(kafkaSystem)) {
            throw new IllegalArgumentException(String.format("No configured kafka properties found for system %s.", kafkaSystem));
        }

        StreamingProperties.DynamicKafka.KafkaSystemProperties config = properties.getKafka()
                .getSystems()
                .get(kafkaSystem);

        if (config.getBootstrapServers() != null) {
            systemProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        }
        if (config.getSsl() != null && config.getSsl().isEnabled()) {
            systemProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
            systemProps.put(SslConfigs.SSL_PROTOCOL_CONFIG, config.getSsl().getProtocol());
            log.debug("SSL config is enabled. Setting up security protocol as [{}] and SSL protocol as [{}]", SecurityProtocol.SSL.name(), config.getSsl()
                    .getProtocol());

            systemProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.getSsl().getTrustStore().getLocation());
            systemProps.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, config.getSsl().getTrustStore().getType());
            systemProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.getSsl().getTrustStore().getPassword());
            systemProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config.getSsl().getKeyStore().getLocation());
            systemProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config.getSsl().getKeyStore().getPassword());
            systemProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, config.getSsl().getKeyStore().getType());
            systemProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, config.getSsl().getKey().getPassword());
        }

        return systemProps;
    }

    private Map<String, Object> getCommonProperties() {
        Map<String, Object> props = new HashMap<>();
        final var producer = properties.getKafka().getProducer();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        props.put(ProducerConfig.ACKS_CONFIG, producer.getAcks());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, producer.isEnableIdempotenceConfig());
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, producer.getMaxInFlightRequests());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, producer.getBatchSize());
        props.put(ProducerConfig.LINGER_MS_CONFIG, producer.getLingerMs());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134_217_728); // 128 MB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60_000);
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 300_000);

        return props;
    }
}
