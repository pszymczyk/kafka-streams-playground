package com.pszymczyk.common;

import com.fasterxml.jackson.annotation.JsonProperty;

record InboxMessage(
        @JsonProperty("senderTime") long senderTime,
        @JsonProperty("inboxTime") long inboxTime,
        @JsonProperty("sender") String sender,
        @JsonProperty("message") String message) {
}
