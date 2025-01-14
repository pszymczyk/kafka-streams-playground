package com.pszymczyk.app8;

public record NonvoluntaryOperation(String user, String operation) implements SomeEvent {

    public static final String NONVOLUNTARY_OPERATION = "nonvoluntary-operation";

    @Override
    public String getType() {
        return NONVOLUNTARY_OPERATION;
    }
}
