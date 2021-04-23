package com.pszymczyk.app4;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class ItemDetails {
    private final String name;
    private final String description;
    private final String price;
    private final String category;

    @JsonCreator
    ItemDetails(
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("price") String price,
        @JsonProperty("category") String category) {
        this.name = name;
        this.description = description;
        this.price = price;
        this.category = category;
    }

    String getName() {
        return name;
    }

    String getPrice() {
        return price;
    }

    String getCategory() {
        return category;
    }

    String getDescription() {
        return description;
    }
}
