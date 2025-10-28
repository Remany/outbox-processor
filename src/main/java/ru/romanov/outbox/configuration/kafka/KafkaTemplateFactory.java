package ru.romanov.outbox.configuration.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class KafkaTemplateFactory {

    private final Map<String, KafkaTemplate<String, String>> kafkaTemplates;

    public KafkaTemplate<String, String> resolveKafkaTemplate(String kafkaSystem) {
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplates.get(kafkaSystem);
        if (kafkaTemplate == null) {
            throw new IllegalArgumentException(String.format("No kafkaTemplate found for system %s", kafkaSystem));
        }
        return kafkaTemplate;
    }
}