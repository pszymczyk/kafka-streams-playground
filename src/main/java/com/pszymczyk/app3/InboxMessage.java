package com.pszymczyk.app3;

import com.fasterxml.jackson.annotation.JsonProperty;

record InboxMessage(@JsonProperty("timestamp") long timestamp, @JsonProperty("message") String message) {
}
