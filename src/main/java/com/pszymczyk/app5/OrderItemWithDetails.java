package com.pszymczyk.app5;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderItemWithDetails {

    private final OrderItem orderItem;
    private final ItemDetails itemDetails;

    @JsonCreator
    public OrderItemWithDetails(
        @JsonProperty("orderItem") OrderItem orderItem,
        @JsonProperty("itemDetails") ItemDetails itemDetails) {
        this.orderItem = orderItem;
        this.itemDetails = itemDetails;
    }

    public OrderItem getOrderItem() {
        return orderItem;
    }

    public ItemDetails getItemDetails() {
        return itemDetails;
    }
}
