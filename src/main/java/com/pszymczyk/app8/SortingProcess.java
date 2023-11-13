package com.pszymczyk.app8;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.pszymczyk.app8.SortingEventsApp.UNSORTED_EVENTS_STORE;


public class SortingProcess implements Processor<String, UnsortedEvent, String, UnsortedEvent> {

    private final long maintainDurationMs = TimeUnit.SECONDS.toMillis(30);

    private KeyValueStore<String, UnsortedEvents> unsortedEventsStore;
    private ProcessorContext<String, UnsortedEvent> context;

    @Override
    public void init(ProcessorContext<String, UnsortedEvent> context) {
        this.context = context;
        this.unsortedEventsStore = context.getStateStore(UNSORTED_EVENTS_STORE);
        context.schedule(Duration.ofMillis(500), PunctuationType.WALL_CLOCK_TIME,
            timestamp -> {
                try (var iterator = unsortedEventsStore.all()) {
                    while (iterator.hasNext()) {
                        var unsortedEvents = iterator.next();
                        if (hasExpired(unsortedEvents.value.getLastModification().toEpochMilli(), timestamp)) {
                            unsortedEventsStore.delete(unsortedEvents.key);
                        }
                    }
                }
            });
    }

    private boolean hasExpired(final long eventTimestamp, final long currentStreamTimeMs) {
        return (currentStreamTimeMs - eventTimestamp) > maintainDurationMs;
    }

    @Override
    public void process(Record<String, UnsortedEvent> record) {
        UnsortedEvents unsortedEvents = this.unsortedEventsStore.get(record.value().processId());
        if (unsortedEvents == null) {
            unsortedEventsStore.put(record.value().processId(), UnsortedEvents.of(record.value()));
            return;
        }

        unsortedEvents.add(record.value());
        if (unsortedEvents.size() >= 3) {
            unsortedEvents.sort();
            unsortedEvents.forEach(unsortedEvent -> context.forward(new Record<>(unsortedEvent.processId(), unsortedEvent,
                context.currentStreamTimeMs())));
            unsortedEventsStore.delete(record.value().processId());
        } else {
            unsortedEventsStore.put(record.value().processId(), unsortedEvents);
        }
    }
}

