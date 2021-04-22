package com.pszymczyk.app2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CommunicationAgreementRevoked implements CustomerPreferencesEvent {

    public static final String TYPE = "CommunicationAgreementRevoked";

    private final String userId;
    private final String revokedAgreement;

    @JsonCreator
    public CommunicationAgreementRevoked(
        @JsonProperty("userId") String userId,
        @JsonProperty("revokedAgreement") String revokedAgreement) {
        this.userId = userId;
        this.revokedAgreement = revokedAgreement;
    }

    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public String getRevokedAgreement() {
        return revokedAgreement;
    }
}
