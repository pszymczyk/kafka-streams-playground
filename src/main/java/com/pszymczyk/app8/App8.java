package com.pszymczyk.app8;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Map;

class App8 {

    static final String SOURCE_TOPIC = "app-source";
    static final String SINK_TOPIC = "app8-sink";
    static final String NONOVOLUNTARY_OPERATIONS_TOPIC = "app8-nonvoluntary-operations";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "app8",
            builder,
            Map.of(),
            new NewTopic(SOURCE_TOPIC, 1, (short) 1),
            new NewTopic(SINK_TOPIC, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, DailyTransactionsLog>> transferProcessKeyValueStore = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("daily-transactions-log"), Serdes.String(), JsonSerdes.newSerdes(DailyTransactionsLog.class));
        builder.addStateStore(transferProcessKeyValueStore);

        builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.Void(), Serdes.String()))
            .selectKey((key, value) -> value.split(",")[2])
            .process(LoanApplicationProcess::new, "daily-transactions-log")
            .split()
            .branch((key, value) -> BusinessTransaction.BUSINESS_TRANSACTION.equals(value.getType()),
                Branched.withConsumer(ks -> ks.mapValues(BusinessTransaction.class::cast)
                    .to(SINK_TOPIC, Produced.with(Serdes.String(), JsonSerdes.newSerdes(BusinessTransaction.class)))))
            .branch(((key, value) -> NonvoluntaryOperation.NONVOLUNTARY_OPERATION.equals(value.getType())),
                Branched.withConsumer(ks -> ks.mapValues(NonvoluntaryOperation.class::cast)
                    .to(NONOVOLUNTARY_OPERATIONS_TOPIC, Produced.with(Serdes.String(), JsonSerdes.newSerdes(NonvoluntaryOperation.class)))))
            .noDefaultBranch();

        return builder;
    }
}