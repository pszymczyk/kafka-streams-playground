package com.pszymczyk.app9;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.pszymczyk.app9.App9.APP_9_STATE;


class SortingProcess implements Processor<String, UnsortedEvent, String, UnsortedEvent> {

    private final long maintainDurationMs = TimeUnit.SECONDS.toMillis(30);

    private KeyValueStore<String, UnsortedEvents> unsortedEventsStore;
    private ProcessorContext<String, UnsortedEvent> context;

    @Override
    public void init(ProcessorContext<String, UnsortedEvent> context) {
        this.context = context;

    }

    private boolean hasExpired(final long eventTimestamp, final long currentStreamTimeMs) {
        return (currentStreamTimeMs - eventTimestamp) > maintainDurationMs;
    }

    @Override
    public void process(Record<String, UnsortedEvent> record) {

    }
}

