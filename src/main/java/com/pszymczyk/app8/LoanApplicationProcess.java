package com.pszymczyk.app8;

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
        context.forward(processApplication(record.value()));
    }

    @Override
    public void close() {

    }


    private Record<String, LoanApplicationDecision> processApplication(LoanApplicationRequest loanApplicationRequest) {
        Integer userLoansCount = usersLoansCount.get(loanApplicationRequest.getRequester());

        if (userLoansCount == null) {
            usersLoansCount.put(loanApplicationRequest.getRequester(), 1);
            LoanApplicationDecision loanApplicationDecision = new LoanApplicationDecision();
            loanApplicationDecision.setAmount(loanApplicationRequest.getAmount());
            loanApplicationDecision.setRequester(loanApplicationRequest.getRequester());
            return new Record<>(loanApplicationDecision.getRequester(), loanApplicationDecision, context.currentStreamTimeMs());
        } else if (userLoansCount == 1) {
            usersLoansCount.put(loanApplicationRequest.getRequester(), 2);
            LoanApplicationDecision loanApplicationDecision = new LoanApplicationDecision();
            loanApplicationDecision.setAmount(loanApplicationRequest.getAmount().multiply(new BigDecimal("0.8")));
            loanApplicationDecision.setRequester(loanApplicationRequest.getRequester());
            return new Record<>(loanApplicationDecision.getRequester(), loanApplicationDecision, context.currentStreamTimeMs());
        } else {
            usersLoansCount.put(loanApplicationRequest.getRequester(), ++userLoansCount);
            LoanApplicationDecision loanApplicationDecision = new LoanApplicationDecision();
            loanApplicationDecision.setAmount(loanApplicationRequest.getAmount().multiply(new BigDecimal("0.5")));
            loanApplicationDecision.setRequester(loanApplicationRequest.getRequester());
            return new Record<>(loanApplicationDecision.getRequester(), loanApplicationDecision, context.currentStreamTimeMs());
        }
    }

}
