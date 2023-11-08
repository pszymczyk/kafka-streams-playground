package com.pszymczyk.app8;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.concurrent.TimeUnit;


public class SortingProcess implements Processor<String, SomeUnsortedEvent, String, SomeUnsortedEvent> {

    private final long maintainDurationMs = TimeUnit.SECONDS.toMillis(30);

    private KeyValueStore<String, SomeUnsortedEvents> unsortedEventsStore;
    private ProcessorContext<String, SomeUnsortedEvent> context;

    @Override
    public void init(ProcessorContext<String, SomeUnsortedEvent> context) {
        this.context = context;
        this.unsortedEventsStore = context.getStateStore("unsorted-events");
    }


    private boolean hasExpired(final long eventTimestamp, final long currentStreamTimeMs) {
        return (currentStreamTimeMs - eventTimestamp) > maintainDurationMs;
    }

    @Override
    public void process(Record<String, SomeUnsortedEvent> record) {
        
    }

    @Override
    public void close() {

    }
}

