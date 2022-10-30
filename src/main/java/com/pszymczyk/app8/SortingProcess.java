package com.pszymczyk.app8;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.concurrent.TimeUnit;


public class SortingProcess implements Transformer<String, SomeUnsortedEvent, KeyValue<String, SomeUnsortedEvent>> {


    private final long maintainDurationMs = TimeUnit.MINUTES.toMillis(5);

    private KeyValueStore<String, SomeUnsortedEvents> unsortedEventsStore;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.unsortedEventsStore = context.getStateStore("unsorted-events");
        context.schedule(Duration.ofMillis(5), PunctuationType.WALL_CLOCK_TIME,
            timestamp -> {
                throw new RuntimeException("TODO");
            });
    }

    @Override
    public KeyValue<String, SomeUnsortedEvent> transform(String key, SomeUnsortedEvent unsortedEvent) {
        throw new RuntimeException("TODO");
    }

    @Override
    public void close() {

    }
}
