package com.pszymczyk.app7;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

class LoanApplicationProcessorApp {

    static final String LOAN_APPLICATION_REQUESTS = "loan-application-requests2";
    static final String LOAN_APPLICATION_DECISIONS = "loan-application-decisions2";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "LoanApplicationProcess-app-main",
            builder,
            Map.of(),
            new NewTopic(LOAN_APPLICATION_REQUESTS, 1, (short) 1),
            new NewTopic(LOAN_APPLICATION_DECISIONS, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, Integer>> transferProcessKeyValueStore = Stores
            .keyValueStoreBuilder(Stores.persistentKeyValueStore("users-loans-count"), Serdes.String(), Serdes.Integer());
        builder.addStateStore(transferProcessKeyValueStore);

        builder.stream(LOAN_APPLICATION_REQUESTS, Consumed.with(Serdes.String(), JsonSerdes.forA(LoanApplicationRequest.class)))
            .transform(LoanApplicationProcess::new, "users-loans-count")
            .to(LOAN_APPLICATION_DECISIONS, Produced.with(Serdes.String(), JsonSerdes.forA(LoanApplicationDecision.class)));

        return builder;
    }
}