package com.pszymczyk.app8;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;


public class SortingProcess implements Transformer<String, SomeUnsortedEvent, KeyValue<String, SomeUnsortedEvent>> {

    private KeyValueStore<String, SomeUnsortedEvents> unsortedEventsStore;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.unsortedEventsStore = context.getStateStore("unsorted-events");
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
