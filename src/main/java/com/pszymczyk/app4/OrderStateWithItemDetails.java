package com.pszymczyk.app4;

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

    OrderStateWithItemDetails apply(ItemAdded value, ItemDetails itemDetails) {
        orderId = value.getOrderId();
        items.put(value.getItem(), items.getOrDefault(value.getItem(), 0L) + 1);
        itemsDetails.put(value.getItem(), itemDetails);
        return this;
    }

    OrderStateWithItemDetails apply(ItemRemoved value) {
        orderId = value.getOrderId();
        items.put(value.getItem(), items.getOrDefault(value.getItem(), 0L) - 1);
        return this;
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
}
