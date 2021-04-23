package com.pszymczyk.app5;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

class OrderStateWithItemDetails {

    private String orderId;
    private Map<String, Long> items;
    private Map<String, ItemDetails> itemsDetails;

    static OrderStateWithItemDetails create() {
        return new OrderStateWithItemDetails(null, new HashMap<>(), new HashMap<>());
    }

    @JsonCreator
    OrderStateWithItemDetails(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("items") Map<String, Long> items,
        @JsonProperty("itemsDetails") Map<String, ItemDetails> itemsDetails) {
        this.orderId = orderId;
        this.items = items;
        this.itemsDetails = itemsDetails;
    }

    String getOrderId() {
        return orderId;
    }

    Map<String, Long> getItems() {
        return items;
    }

    Map<String, ItemDetails> getItemsDetails() {
        return itemsDetails;
    }

    OrderStateWithItemDetails add(OrderItemWithDetails orderItemWithDetails) {
        items.put(orderItemWithDetails.getOrderItem().getItem(), orderItemWithDetails.getOrderItem().getCount());
        itemsDetails.put(orderItemWithDetails.getOrderItem().getItem(), orderItemWithDetails.getItemDetails());
        return this;
    }
}
