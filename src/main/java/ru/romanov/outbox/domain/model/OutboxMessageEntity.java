package ru.romanov.outbox.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.romanov.outbox.domain.enums.OutboxMessageStatus;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxMessageEntity {
    private UUID id;
    private String key;
    private String topic;
    private String headers;
    private String payload;
    private String kafkaSystem;
    private OutboxMessageStatus status;
    private LocalDateTime reservedTo;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
