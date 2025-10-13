package ru.romanov.outbox.storage;

import ru.romanov.outbox.domain.enums.OutboxMessageStatus;

import java.time.LocalDateTime;
import java.util.UUID;

public record UpdateEntry(MessageRef ref, OutboxMessageStatus status, LocalDateTime reservedTo) {

    public static UpdateEntry of(UUID id, OutboxMessageStatus status) {
        final var ref = MessageRef.of(id);
        return new UpdateEntry(ref, status, null);
    }

    public static UpdateEntry of(UUID id, OutboxMessageStatus status, LocalDateTime reservedTo) {
        final var ref = MessageRef.of(id);
        return new UpdateEntry(ref, status, reservedTo);
    }

    public static UpdateEntry of(MessageRef ref, OutboxMessageStatus status) {
        return new UpdateEntry(ref, status, null);
    }

    public static UpdateEntry of(MessageRef ref, OutboxMessageStatus status, LocalDateTime reservedTo) {
        return new UpdateEntry(ref, status, reservedTo);
    }
}
