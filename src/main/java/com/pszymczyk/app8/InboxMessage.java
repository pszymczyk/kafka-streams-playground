package com.pszymczyk.app8;

import com.fasterxml.jackson.annotation.JsonProperty;

record InboxMessage(
        @JsonProperty("senderTime") long senderTime,
        @JsonProperty("inboxTime") long inboxTime,
        @JsonProperty("sender") String sender,
        @JsonProperty("sender") String messageId,
        @JsonProperty("message") String message) {
}
