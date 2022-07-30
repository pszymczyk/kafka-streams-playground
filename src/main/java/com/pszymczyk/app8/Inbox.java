package com.pszymczyk.app8;

import com.pszymczyk.common.Message;

import java.time.Instant;
import java.util.List;

record Inbox(List<InboxMessage> messages) {

    public InboxMessage add(int partition, long offset, Message message) {
        InboxMessage inboxMessage = new InboxMessage(message.timestamp(),
                Instant.now().toEpochMilli(),
                message.sender(),
                String.format("%d-%d", partition, offset),
                message.value());
        messages.add(inboxMessage);
        return inboxMessage;
    }
}
