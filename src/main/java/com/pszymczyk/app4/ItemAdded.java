package com.pszymczyk.app4;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemAdded implements OrderEvent {

    public static final String TYPE = "ItemAdded";

    private final String orderId;
    private final String item;

    @JsonCreator
    public ItemAdded(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("item") String item) {
        this.orderId = orderId;
        this.item = item;
    }

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
