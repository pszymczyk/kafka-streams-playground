package com.pszymczyk.app4;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemDetails {
    private final String name;
    private final String description;
    private final String price;
    private final String category;

    @JsonCreator
    public ItemDetails(
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("price") String price,
        @JsonProperty("category") String category) {
        this.name = name;
        this.description = description;
        this.price = price;
        this.category = category;
    }

    public String getName() {
        return name;
    }

    public String getPrice() {
        return price;
    }

    public String getCategory() {
        return category;
    }

    public String getDescription() {
        return description;
    }
}
