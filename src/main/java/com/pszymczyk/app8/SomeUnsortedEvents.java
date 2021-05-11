package com.pszymczyk.app8;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

public class SomeUnsortedEvents {

    private List<SomeUnsortedEvent> someUnsortedEvents;
    private Instant lastModification;

    public static SomeUnsortedEvents of(SomeUnsortedEvent unsortedEvent) {
        SomeUnsortedEvents someUnsortedEvents = new SomeUnsortedEvents();
        someUnsortedEvents.someUnsortedEvents = List.of(unsortedEvent);
        someUnsortedEvents.lastModification = Instant.now();
        return someUnsortedEvents;
    }

    public List<SomeUnsortedEvent> getSomeUnsortedEvents() {
        return someUnsortedEvents;
    }

    public void setSomeUnsortedEvents(List<SomeUnsortedEvent> someUnsortedEvents) {
        this.someUnsortedEvents = someUnsortedEvents;
    }

    public void add(SomeUnsortedEvent unsortedEvent) {
        someUnsortedEvents.add(unsortedEvent);
        lastModification = Instant.now();
    }

    public int size() {
        return someUnsortedEvents.size();
    }

    public void sort() {
        someUnsortedEvents.sort(Comparator.comparing(SomeUnsortedEvent::getOrder));
    }

    public void forEach(Consumer<SomeUnsortedEvent> consumer) {
        someUnsortedEvents.forEach(consumer);
    }

    public Instant getLastModification() {
        return lastModification;
    }
}
