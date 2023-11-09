package com.pszymczyk.app8;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.pszymczyk.app8.SortingEventsApp.UNSORTED_EVENTS_STORE;


public class SortingProcess implements Processor<String, SomeUnsortedEvent, String, SomeUnsortedEvent> {

    private final long maintainDurationMs = TimeUnit.SECONDS.toMillis(30);

    private KeyValueStore<String, SomeUnsortedEvents> unsortedEventsStore;
    private ProcessorContext<String, SomeUnsortedEvent> context;

    @Override
    public void init(ProcessorContext<String, SomeUnsortedEvent> context) {
        this.context = context;
        this.unsortedEventsStore = context.getStateStore(UNSORTED_EVENTS_STORE);
        context.schedule(Duration.ofMillis(5), PunctuationType.WALL_CLOCK_TIME,
            timestamp -> unsortedEventsStore.all()
                .forEachRemaining(kV -> {
                    if (hasExpired(kV.value.getLastModification().toEpochMilli(), timestamp)) {
                        unsortedEventsStore.delete(kV.key);
                    }
                }));
    }

    private boolean hasExpired(final long eventTimestamp, final long currentStreamTimeMs) {
        return (currentStreamTimeMs - eventTimestamp) > maintainDurationMs;
    }

    @Override
    public void process(Record<String, SomeUnsortedEvent> record) {
        SomeUnsortedEvents unsortedEvents = this.unsortedEventsStore.get(record.value().getProcessId());
        if (unsortedEvents == null) {
            unsortedEventsStore.put(record.value().getProcessId(), SomeUnsortedEvents.of(record.value()));
            return;
        }

        unsortedEvents.add(record.value());
        if (unsortedEvents.size() >= 3) {
            unsortedEvents.sort();
            unsortedEvents.forEach(someUnsortedEvent -> context.forward(new Record<>(record.value().getProcessId(), record.value(),
                context.currentStreamTimeMs())));
            unsortedEventsStore.delete(record.value().getProcessId());
        } else {
            unsortedEventsStore.put(record.value().getProcessId(), unsortedEvents);
        }
    }

    @Override
    public void close() {

    }
}

