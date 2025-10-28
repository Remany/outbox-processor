package ru.romanov.outbox.storage.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.storage.OutboxMessageQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxMessageQueueImpl implements OutboxMessageQueue {

    private final BlockingQueue<OutboxMessageEntity> newMessages;

    public OutboxMessageQueueImpl() {
        this.newMessages = new LinkedBlockingQueue<>();
    }

    @Override
    public void add(OutboxMessageEntity message) {
        log.debug("Сообщение добавлено в очередь на отправку через outbox, key: [{}]", message.getKey());
        newMessages.add(message);
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
    public OutboxMessageEntity take() throws InterruptedException {
        return newMessages.take();
    }
}
