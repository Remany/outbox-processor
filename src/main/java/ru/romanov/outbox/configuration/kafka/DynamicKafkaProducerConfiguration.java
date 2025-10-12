package ru.romanov.outbox.configuration.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
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

    @Bean
    public Map<String, KafkaTemplate<String, String>> kafkaTemplates() {
        Map<String, KafkaTemplate<String, String>> templates = new HashMap<>();
        if (properties.getKafka().getSystems() != null) {
            for (String kafkaSystem : properties.getKafka().getSystems().keySet()) {
                templates.put(kafkaSystem, createKafkaTemplate(kafkaSystem));
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

    private KafkaTemplate<String, String> createKafkaTemplate(String kafkaSystem) {
        Map<String, Object> combinedProps = new HashMap<>(getCommonProperties());

        Map<String, Object> systemProps = getSystemProperties(kafkaSystem);
        combinedProps.putAll(systemProps);

        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(combinedProps);
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
        Map<String, Object> commonProps = new HashMap<>();

        commonProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        commonProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        commonProps.put(ProducerConfig.ACKS_CONFIG, "all");
        commonProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        commonProps.put(ProducerConfig.BATCH_SIZE_CONFIG, properties.getKafka().getProducer().getBatchSize());
        commonProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, properties.getKafka()
                .getProducer()
                .getDeliveryTimeoutMs());

        return commonProps;
    }
}
