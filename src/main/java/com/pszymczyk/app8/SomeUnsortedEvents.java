package com.pszymczyk.app8;

import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

public class SomeUnsortedEvents {

    private List<SomeUnsortedEvent> someUnsortedEvents;

    public static SomeUnsortedEvents of(SomeUnsortedEvent unsortedEvent) {
        SomeUnsortedEvents someUnsortedEvents = new SomeUnsortedEvents();
        someUnsortedEvents.someUnsortedEvents = List.of(unsortedEvent);
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
}
