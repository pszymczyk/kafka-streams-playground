package com.pszymczyk.app7;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;


public class LoanApplicationProcess implements Transformer<String, LoanApplicationRequest, KeyValue<String, LoanApplicationDecision>> {

    private KeyValueStore<String, Integer> usersLoansCount;

    @Override
    public void init(ProcessorContext context) {
        usersLoansCount = context.getStateStore("users-loans-count");
    }

    @Override
    public KeyValue<String, LoanApplicationDecision> transform(String key, LoanApplicationRequest value) {
        return processApplication(value);
    }

    @Override
    public void close() {

    }


    private KeyValue<String, LoanApplicationDecision> processApplication(LoanApplicationRequest loanApplicationRequest) {
        Integer userLoansCount = usersLoansCount.get(loanApplicationRequest.getRequester());

        if (userLoansCount == null) {
            usersLoansCount.put(loanApplicationRequest.getRequester(), 1);
            LoanApplicationDecision loanApplicationDecision = new LoanApplicationDecision();
            loanApplicationDecision.setAmount(loanApplicationRequest.getAmount());
            loanApplicationDecision.setRequester(loanApplicationRequest.getRequester());
            return new KeyValue<>(loanApplicationDecision.getRequester(), loanApplicationDecision);
        } else if (userLoansCount == 1) {
            usersLoansCount.put(loanApplicationRequest.getRequester(), 2);
            LoanApplicationDecision loanApplicationDecision = new LoanApplicationDecision();
            loanApplicationDecision.setAmount(loanApplicationRequest.getAmount().multiply(new BigDecimal("0.8")));
            loanApplicationDecision.setRequester(loanApplicationRequest.getRequester());
            return new KeyValue<>(loanApplicationDecision.getRequester(), loanApplicationDecision);
        } else {
            usersLoansCount.put(loanApplicationRequest.getRequester(), ++userLoansCount);
            LoanApplicationDecision loanApplicationDecision = new LoanApplicationDecision();
            loanApplicationDecision.setAmount(loanApplicationRequest.getAmount().multiply(new BigDecimal("0.5")));
            loanApplicationDecision.setRequester(loanApplicationRequest.getRequester());
            return new KeyValue<>(loanApplicationDecision.getRequester(), loanApplicationDecision);
        }
    }

}
