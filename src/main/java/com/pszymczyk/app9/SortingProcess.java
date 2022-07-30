package com.pszymczyk.app9;

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
    public KeyValue<String, SomeUnsortedEvent> transform(String key, SomeUnsortedEvent unsortedEvent) {
        SomeUnsortedEvents unsortedEvents = this.unsortedEventsStore.get(unsortedEvent.getProcessId());
        if (unsortedEvents == null) {
            unsortedEventsStore.put(unsortedEvent.getProcessId(), SomeUnsortedEvents.of(unsortedEvent));
            return null;
        }

        unsortedEvents.add(unsortedEvent);
        if (unsortedEvents.size() >= 3) {
            unsortedEvents.sort();
            unsortedEvents.forEach(someUnsortedEvent -> context.forward(someUnsortedEvent.getProcessId(), someUnsortedEvent));
            unsortedEventsStore.delete(unsortedEvent.getProcessId());
        } else {
            unsortedEventsStore.put(unsortedEvent.getProcessId(), unsortedEvents);
        }
        return null;
    }

    @Override
    public void close() {

    }
}
