package com.pszymczyk.app5;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class OrderItemWithDetails {

    private final OrderItem orderItem;
    private final ItemDetails itemDetails;

    @JsonCreator
    OrderItemWithDetails(
        @JsonProperty("orderItem") OrderItem orderItem,
        @JsonProperty("itemDetails") ItemDetails itemDetails) {
        this.orderItem = orderItem;
        this.itemDetails = itemDetails;
    }

    OrderItem getOrderItem() {
        return orderItem;
    }

    ItemDetails getItemDetails() {
        return itemDetails;
    }
}
