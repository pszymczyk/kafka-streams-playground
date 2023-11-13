package com.pszymczyk.common;

public record Message(
    Long timestamp,
    String sender,
    String receiver,
    String value) {
}
