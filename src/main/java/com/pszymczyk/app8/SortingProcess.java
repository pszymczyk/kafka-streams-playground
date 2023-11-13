package com.pszymczyk.app8;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import java.util.concurrent.TimeUnit;


public class SortingProcess implements Processor<String, UnsortedEvent, String, UnsortedEvent> {

    private final long maintainDurationMs = TimeUnit.SECONDS.toMillis(30);


    @Override
    public void init(ProcessorContext<String, UnsortedEvent> context) {

    }

    private boolean hasExpired(final long eventTimestamp, final long currentStreamTimeMs) {
        return (currentStreamTimeMs - eventTimestamp) > maintainDurationMs;
    }

    @Override
    public void process(Record<String, UnsortedEvent> record) {

    }
}

