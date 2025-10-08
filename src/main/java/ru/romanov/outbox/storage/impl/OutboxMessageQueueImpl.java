package ru.romanov.outbox.storage.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.storage.OutboxMessageQueue;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxMessageQueueImpl implements OutboxMessageQueue {

    private final BlockingQueue<OutboxMessageEntity> newMessages;

    private final BlockingQueue<OutboxMessageEntity> successQueue;

    private final BlockingQueue<OutboxMessageEntity> failureQueue;

    public OutboxMessageQueueImpl() {
        this.newMessages = new LinkedBlockingQueue<>();
        this.successQueue = new LinkedBlockingQueue<>();
        this.failureQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void add(OutboxMessageEntity message) {
        log.debug("Сообщение добавлено в очередь на отправку через outbox, key: [{}]", message.getKey());
        addWithContainsCheck(newMessages, message);
    }

    @Override
    public void addAfterTxCommit(OutboxMessageEntity message) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                add(message);
            }
        });
    }

    @Override
    public OutboxMessageEntity take(OutboxMessageStatus status) throws InterruptedException {
        return switch (status) {
            case NEW -> newMessages.take();
            case SUCCESS -> successQueue.take();
            case FAILURE -> failureQueue.take();
        };
    }

    @Override
    public void markSuccess(OutboxMessageEntity message) {
        log.debug("Сообщение добавлено в очередь успешно отправленных сообщений, key: [{}]", message.getKey());
        addWithContainsCheck(successQueue, message);
    }

    @Override
    public void markFailure(OutboxMessageEntity message) {
        log.debug("Сообщение добавлено в очередь не отправленных сообщений, key: [{}]", message.getKey());
        addWithContainsCheck(failureQueue, message);
    }

    private void addWithContainsCheck(Queue<OutboxMessageEntity> queue, OutboxMessageEntity message) {
        if (message != null && !newMessages.contains(message)) {
            queue.add(message);
        }
    }
}
