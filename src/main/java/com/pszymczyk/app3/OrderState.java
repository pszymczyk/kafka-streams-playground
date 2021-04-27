package com.pszymczyk.app3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OrderState {

    private final Map<String, Long> items;

    private String orderId;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrderState that = (OrderState) o;
        return items.equals(that.items) && Objects.equals(orderId, that.orderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(items, orderId);
    }
}
