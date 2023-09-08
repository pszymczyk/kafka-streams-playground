package com.pszymczyk.app7;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;


public class LoanApplicationProcess implements Processor<String, LoanApplicationRequest, String, LoanApplicationDecision> {

    private KeyValueStore<String, Integer> usersLoansCount;
    private ProcessorContext<String, LoanApplicationDecision> context;

    @Override
    public void init(ProcessorContext<String, LoanApplicationDecision> context) {
        this.context = context;
        usersLoansCount = context.getStateStore("users-loans-count");
    }

    @Override
    public void process(Record<String, LoanApplicationRequest> record) {
    }

    @Override
    public void close() {

    }
}
