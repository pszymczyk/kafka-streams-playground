package com.pszymczyk.app4;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class EnrichedOrderEvent {

    private final ItemDetails itemDetails;
    private final OrderEvent orderEvent;

    @JsonCreator
    EnrichedOrderEvent(
        @JsonProperty("itemDetails") ItemDetails itemDetails,
        @JsonProperty("orderEvent") OrderEvent orderEvent) {
        this.itemDetails = itemDetails;
        this.orderEvent = orderEvent;
    }

    ItemDetails getItemDetails() {
        return itemDetails;
    }

    OrderEvent getOrderEvent() {
        return orderEvent;
    }
}
