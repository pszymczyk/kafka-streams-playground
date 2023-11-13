package com.pszymczyk.app8;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

public class UnsortedEvents {

    private List<UnsortedEvent> unsortedEvents;
    private Instant lastModification;

    public static UnsortedEvents of(UnsortedEvent unsortedEvent) {
        UnsortedEvents someUnsortedEvents = new UnsortedEvents();
        someUnsortedEvents.unsortedEvents = List.of(unsortedEvent);
        someUnsortedEvents.lastModification = Instant.now();
        return someUnsortedEvents;
    }

    public List<UnsortedEvent> getSomeUnsortedEvents() {
        return unsortedEvents;
    }

    public void setSomeUnsortedEvents(List<UnsortedEvent> unsortedEvents) {
        this.unsortedEvents = unsortedEvents;
    }

    public void add(UnsortedEvent unsortedEvent) {
        unsortedEvents.add(unsortedEvent);
        lastModification = Instant.now();
    }

    public int size() {
        return unsortedEvents.size();
    }

    public void sort() {
        unsortedEvents.sort(Comparator.comparing(UnsortedEvent::order));
    }

    public void forEach(Consumer<UnsortedEvent> consumer) {
        unsortedEvents.forEach(consumer);
    }

    public Instant getLastModification() {
        return lastModification;
    }
}
