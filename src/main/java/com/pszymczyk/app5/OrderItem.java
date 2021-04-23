package com.pszymczyk.app5;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class OrderItem {

    private String orderId;
    private String item;
    private Long count;

    static OrderItem create() {
        return new OrderItem(null, null, 0L);
    }

    @JsonCreator
    public OrderItem(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("item") String item,
        @JsonProperty("count") Long count) {
        this.orderId = orderId;
        this.item = item;
        this.count = count;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getItem() {
        return item;
    }

    public Long getCount() {
        return count;
    }

    public OrderItem apply(ItemAdded value) {
        orderId = value.getOrderId();
        item = value.getItem();
        count = ++count;
        return this;
    }

    public OrderItem apply(ItemRemoved value) {
        long newCount = --count;
        if (newCount <= 0) {
            return null;
        }
        return this;
    }
}
