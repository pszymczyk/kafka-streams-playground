package com.pszymczyk.app5;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class ItemAdded implements OrderEvent {

    static final String TYPE = "ItemAdded";

    private final String orderId;
    private final String item;

    @JsonCreator
    ItemAdded(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("item") String item) {
        this.orderId = orderId;
        this.item = item;
    }

    @Override
    public String getItem() {
        return item;
    }

    @Override
    public String getOrderId() {
        return orderId;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
