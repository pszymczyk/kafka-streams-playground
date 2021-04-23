package com.pszymczyk.app5;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class OrderStateWithItemDetails {

    private String orderId;
    private Map<String, Long> items;
    private Map<String, ItemDetails> itemsDetails;

    static OrderStateWithItemDetails create() {
        return new OrderStateWithItemDetails(null, new HashMap<>(), new HashMap<>());
    }

    @JsonCreator
    public OrderStateWithItemDetails(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("items") Map<String, Long> items,
        @JsonProperty("itemsDetails") Map<String, ItemDetails> itemsDetails) {
        this.orderId = orderId;
        this.items = items;
        this.itemsDetails = itemsDetails;
    }

    public String getOrderId() {
        return orderId;
    }

    public Map<String, Long> getItems() {
        return items;
    }

    public Map<String, ItemDetails> getItemsDetails() {
        return itemsDetails;
    }

    public OrderStateWithItemDetails add(OrderItemWithDetails orderItemWithDetails) {
        items.put(orderItemWithDetails.getOrderItem().getItem(), orderItemWithDetails.getOrderItem().getCount());
        itemsDetails.put(orderItemWithDetails.getOrderItem().getItem(), orderItemWithDetails.getItemDetails());
        return this;
    }
}
