package com.pszymczyk.app8;

public record BusinessTransaction(String type, String user, String stock, int number) implements SomeEvent{

    public static final String BUSINESS_TRANSACTION = "business-transaction";
    public static String BUY = "buy";
    public static String SELL = "sell";

    public static BusinessTransaction buy(String user, String stock, int number) {
        return new BusinessTransaction(BUY, user, stock, number);
    }

    public static BusinessTransaction sell(String user, String stock, int number) {
        return new BusinessTransaction(SELL, user, stock, number);
    }

    @Override
    public String getType() {
        return BUSINESS_TRANSACTION;
    }
}
