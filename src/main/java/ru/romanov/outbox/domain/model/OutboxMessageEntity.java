package ru.romanov.outbox.domain.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OutboxMessageEntity {
    private UUID id;
    private String key;
    private JsonNode headers;
    private JsonNode payload;
    private String kafkaSystem;
    private String topic;
    private LocalDateTime reservedTo;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
