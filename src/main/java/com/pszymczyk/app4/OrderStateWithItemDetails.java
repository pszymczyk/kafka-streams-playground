package com.pszymczyk.app4;

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

    public OrderStateWithItemDetails apply(ItemAdded value, ItemDetails itemDetails) {
        orderId = value.getOrderId();
        items.put(value.getItem(), items.getOrDefault(value.getItem(), 0L) + 1);
        itemsDetails.put(value.getItem(), itemDetails);
        return this;
    }

    public OrderStateWithItemDetails apply(ItemRemoved value) {
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

    public Map<String, ItemDetails> getItemsDetails() {
        return itemsDetails;
    }
}
