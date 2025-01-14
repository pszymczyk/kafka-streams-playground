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

    //TODO some common interface for events
    @Override
    public void init(ProcessorContext<String, SomeEvent> context) {
        this.context = context;
        this.dailyTransactionsLogKeyValueStore = context.getStateStore("daily-transactions-log");
    }

    @Override
    public void process(Record<String, String> record) {
        String[] split = record.value().split(",");
        String buyer = split[1];
        String seller = split[2];
        String stock = split[3];
        int number = Integer.parseInt(split[4]);

        List.of(record.withKey(buyer).withValue(BusinessTransaction.buy(buyer, stock, number)),
                record.withKey(seller).withValue(BusinessTransaction.sell(seller, stock, number)))
            .forEach(x -> context.forward(x));

        DailyTransactionsLog dailyTransactionsLog =
            Optional.ofNullable(dailyTransactionsLogKeyValueStore.get(seller)).orElse(new DailyTransactionsLog(new HashMap<>()));

        //TODO get date from file name in next iteration
        LocalDate now = LocalDate.now();
        int updatedOperationsCount = dailyTransactionsLog.log().getOrDefault(now, 0) + 1;
        if (updatedOperationsCount > 2) {
            context.forward(record.withKey(seller).withValue(new NonvoluntaryOperation(seller, "sell")));
        }

        dailyTransactionsLog.log().put(now, updatedOperationsCount);
        dailyTransactionsLogKeyValueStore.put(seller, dailyTransactionsLog);
    }

    @Override
    public void close() {

    }

}
