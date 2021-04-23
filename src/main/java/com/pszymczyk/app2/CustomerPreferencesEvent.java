package com.pszymczyk.app2;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = CommunicationAgreementRevoked.class, name = CommunicationAgreementRevoked.TYPE),
    @JsonSubTypes.Type(value = PaymentMethodChanged.class, name = PaymentMethodChanged.TYPE),
    @JsonSubTypes.Type(value = PreferredLocationChanged.class, name = PreferredLocationChanged.TYPE)
})
interface CustomerPreferencesEvent {
    String getUserId();
    String getType();
}
