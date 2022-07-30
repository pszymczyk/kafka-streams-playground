package com.pszymczyk.common;

import java.time.Instant;
import java.util.List;

public record Inbox(List<InboxMessage> messages) {

    public Inbox add(Message message) {
        messages.add(new InboxMessage(message.timestamp(), Instant.now().toEpochMilli(), message.sender(), message.value()));
        return this;
    }
}
