package ru.romanov.outbox.storage;

import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.storage.impl.OutboxMessageQueueImpl;

import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class OutboxMessageQueueTest {

    private OutboxMessageQueue queue;

    @Mock
    private OutboxMessageEntity message;

    @BeforeEach
    void setUp() {
        this.queue = new OutboxMessageQueueImpl();
    }

    @Test
    @SneakyThrows
    void when_add_and_then_callTake_shouldReturnMessage() {
        /* when */
        queue.add(message);

        /* then */
        var actual = queue.take(OutboxMessageStatus.NEW);
        assertEquals(message, actual);
    }

    @Test
    @SneakyThrows
    void when_alreadyExistedMessage_isAdded_thenMessageSilentlyIgnored() {
        /* when */
        queue.add(message);
        queue.add(message);

        /* then */
        Field queueField = queue.getClass().getDeclaredField("newMessages");
        queueField.setAccessible(true);
        BlockingQueue<OutboxMessageEntity> newMessages = (BlockingQueue<OutboxMessageEntity>) queueField.get(queue);
        int expectedSize = 1;
        assertEquals(expectedSize, newMessages.size());
    }
}