package ru.romanov.outbox.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;
import ru.romanov.outbox.metric.OutboxMetrics;
import ru.romanov.outbox.service.OutboxMessageService;
import ru.romanov.outbox.storage.OutboxMessageQueue;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
public class OutboxMessageServiceImpl implements OutboxMessageService {

    private static final long RESERVED_TO_SECONDS = 60;

    private final StreamingProperties streamingProperties;
    private final OutboxMessageRepository repository;
    private final OutboxMessageQueue outboxQueue;
    private final OutboxMetrics outboxMetrics;
    private final ObjectMapper objectMapper;

    @Override
    @SneakyThrows
    @Transactional
    public void save(String key, Object data, Map<String, String> platformToTopic, Map<String, String> headers) {
        Objects.requireNonNull(data);

        final String _key = key != null ? key : UUID.randomUUID().toString();
        final String _value = objectMapper.writeValueAsString(data);

        final String platform = platformToTopic.keySet().stream().findFirst()
                // todo
                .orElseThrow(() -> new RuntimeException("Значение platformToTopic не заполнено"));
        final String topic = platformToTopic.get(platform);

        if (topic == null || topic.isBlank()) {
            // todo
            throw new RuntimeException(String.format("Не найдено значение поля 'topic' для системы [%s]", platform));
        }

        final String _headers = objectMapper.writeValueAsString(headers);

        final OutboxMessageEntity message = OutboxMessageEntity.builder()
                .id(UUID.randomUUID())
                .key(_key)
                .payload(_value)
                .headers(_headers)
                .topic(topic)
                .kafkaSystem(platform)
                .createTime(LocalDateTime.now())
                .updateTime(LocalDateTime.now())
                .reservedTo(LocalDateTime.now().plusSeconds(RESERVED_TO_SECONDS))
                .status(OutboxMessageStatus.NEW)
                .build();

        try {
            /* Сохраняем сообщение в БД и после коммита транзакции помещаем сообщение в очередь воркеров */
            repository.save(message);
            outboxMetrics.messageStored();
            if (streamingProperties.getOutbox().isStreamingEnabled()) {
                outboxQueue.addAfterTxCommit(message);
            }
        } catch (Exception e) {
            log.error("Ошибка сохранения сообщения [{}] в таблицу [{}]", data.getClass()
                    .getSimpleName(), streamingProperties.getOutbox().getTableName(), e);
        }
    }
}
