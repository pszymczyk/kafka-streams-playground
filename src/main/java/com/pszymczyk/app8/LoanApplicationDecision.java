package com.pszymczyk.app8;

import java.math.BigDecimal;

public class LoanApplicationDecision {
    private BigDecimal amount;
    private String requester;

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getRequester() {
        return requester;
    }

    public void setRequester(String requester) {
        this.requester = requester;
    }
}
