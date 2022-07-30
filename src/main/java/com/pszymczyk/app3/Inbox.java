package com.pszymczyk.app3;

import java.time.Instant;
import java.util.List;

record Inbox(List<InboxMessage> messages) {

    Inbox add(Message message) {
        messages.add(new InboxMessage(Instant.now().toEpochMilli(), message.value()));
        return this;
    }
}
