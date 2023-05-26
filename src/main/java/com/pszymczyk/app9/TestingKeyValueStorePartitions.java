package com.pszymczyk.app9;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;


public class TestingKeyValueStorePartitions implements Processor<String, String, String, String> {

    private KeyValueStore<String, String> store;

    private final String instanceId;

    public TestingKeyValueStorePartitions(String instanceId) {
        this.instanceId = instanceId;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.store = context.getStateStore("unsorted-events");
    }

    @Override
    public void process(Record<String, String> record) {
        System.err.println("Instance id " + instanceId + " has " + store.approximateNumEntries());
        store.put(record.value(), instanceId);
    }

    @Override
    public void close() {

    }
}

