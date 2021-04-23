package com.pszymczyk.app2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class PreferredLocationChanged implements CustomerPreferencesEvent {
    static final String TYPE = "PreferredLocationChanged";

    private final String userId;
    private final String newLocation;

    @JsonCreator
    PreferredLocationChanged(
        @JsonProperty("userId") String userId,
        @JsonProperty("newLocation") String newLocation) {
        this.userId = userId;
        this.newLocation = newLocation;
    }

    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public String getNewLocation() {
        return newLocation;
    }
}
