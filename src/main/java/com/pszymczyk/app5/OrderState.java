package com.pszymczyk.app5;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class OrderState {

    private String orderId;
    private Map<String, Long> items;

    static OrderState create() {
        return new OrderState(null, new HashMap<>());
    }

    @JsonCreator
    public OrderState(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("items") Map<String, Long> items) {
        this.orderId = orderId;
        this.items = items;
    }

    public OrderState apply(ItemAdded value) {
        orderId = value.getOrderId();
        items.put(value.getItem(), items.getOrDefault(value.getItem(), 0L) + 1);
        return this;
    }

    public OrderState apply(ItemRemoved value) {
        orderId = value.getOrderId();
        items.put(value.getItem(), items.getOrDefault(value.getItem(), 0L) - 1);
        return this;
    }

    public String getOrderId() {
        return orderId;
    }

    public Map<String, Long> getItems() {
        return items;
    }
}
