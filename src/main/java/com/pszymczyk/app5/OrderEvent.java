package com.pszymczyk.app5;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ItemAdded.class, name = ItemAdded.TYPE),
    @JsonSubTypes.Type(value = ItemRemoved.class, name = ItemRemoved.TYPE)
})
interface OrderEvent {
    String getOrderId();
    String getItem();
    String getType();
}
