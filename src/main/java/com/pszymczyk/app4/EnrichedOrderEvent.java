package com.pszymczyk.app4;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EnrichedOrderEvent {

    private final ItemDetails itemDetails;
    private final OrderEvent orderEvent;

    @JsonCreator
    public EnrichedOrderEvent(
        @JsonProperty("itemDetails") ItemDetails itemDetails,
        @JsonProperty("orderEvent") OrderEvent orderEvent) {
        this.itemDetails = itemDetails;
        this.orderEvent = orderEvent;
    }

    public ItemDetails getItemDetails() {
        return itemDetails;
    }

    public OrderEvent getOrderEvent() {
        return orderEvent;
    }
}
