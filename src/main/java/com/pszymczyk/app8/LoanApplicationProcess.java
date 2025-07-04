package com.pszymczyk.app8;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;


public class LoanApplicationProcess implements Processor<String, String, String, SomeEvent> {

    private KeyValueStore<String, DailyTransactionsLog> dailyTransactionsLogKeyValueStore;
    private ProcessorContext<String, SomeEvent> context;

    @Override
    public void init(ProcessorContext<String, SomeEvent> context) {
        this.context = context;
        this.dailyTransactionsLogKeyValueStore = context.getStateStore("daily-transactions-log");
    }

    @Override
    public void process(Record<String, String> record) {

    }

    @Override
    public void close() {

    }

}
