package com.pszymczyk.app2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PaymentMethodChanged implements CustomerPreferencesEvent {

    public static final String TYPE = "PaymentMethodChanged";

    private final String userId;
    private final String newPaymentMethod;

    @JsonCreator
    public PaymentMethodChanged(
        @JsonProperty("userId") String userId,
        @JsonProperty("newPaymentMethod") String newPaymentMethod) {
        this.userId = userId;
        this.newPaymentMethod = newPaymentMethod;
    }

    public String getNewPaymentMethod() {
        return newPaymentMethod;
    }

    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
