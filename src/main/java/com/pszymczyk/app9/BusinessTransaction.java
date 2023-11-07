package com.pszymczyk.app9;

record BusinessTransaction(String type, String user, String stock, int number) {
    public static String BUY = "buy";
    public static String SELL = "sell";

    public static BusinessTransaction buy(String user, String stock, int number) {
        return new BusinessTransaction(BUY, user, stock, number);
    }

    public static BusinessTransaction sell(String user, String stock, int number) {
        return new BusinessTransaction(SELL, user, stock, number);
    }
}
