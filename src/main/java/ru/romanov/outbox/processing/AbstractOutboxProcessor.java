package ru.romanov.outbox.processing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.service.OutboxMessageStatusService;
import ru.romanov.outbox.storage.MessageRef;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractOutboxProcessor {

    private static final TypeReference<Map<String, String>> HEADER_TYPE = new TypeReference<>() {
    };

    protected final ObjectMapper objectMapper;
    protected final OutboxMessageStatusService statusService;

    public abstract void processingMessages();

    protected void handleErrorRef(MessageRef ref, Throwable ex) {
        log.error("Не получилось отправить сообщение в Kafka, система потребитель: [{}], topic: [{}], key: [{}], подробности: [{}]",
                ref.kafkaSystem(), ref.topic(), ref.key(), ex.getMessage());
        statusService.addFailure(ref);
    }

    protected ProducerRecord<String, String> createRecord(OutboxMessageEntity message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(message.getTopic(), message.getKey(), message.getPayload());
        if (message.getHeaders() != null) {
            var _headers = parseHeaders(message.getHeaders());
            _headers.forEach((k, v) -> record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))));
        }
        return record;
    }

    protected Map<String, String> parseHeaders(String headers) {
        try {
            return objectMapper.readValue(headers, HEADER_TYPE);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error while trying to parse headers", e);
        }
    }

    protected String classifyError(Throwable ex) {
        if (ex == null) return "unknown";

        String className = ex.getClass().getSimpleName().toLowerCase();

        if (className.contains("timeout")) return "timeout";
        if (className.contains("network")) return "network_error";
        if (className.contains("serialization")) return "serialization_error";
        if (className.contains("authorization")) return "auth_error";
        if (className.contains("authentication")) return "auth_error";
        if (className.contains("coordinator")) return "coordinator_error";

        return "other";
    }

    protected void logErr(String details) {
        log.error("Ошибка в работе outbox worker, подробности: [{}]", details);
    }
}
