package com.pszymczyk.tx2;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.List;
import java.util.Map;

public class Tx2Runner {

    static final String INPUT = "tx2-input";
    static final String OUTPUT = "tx2-output";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "tx2-runner-main",
            builder,
            Map.of(),
            new NewTopic(INPUT, 1, (short) 1),
            new NewTopic(OUTPUT, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.String()))
            .flatMap((key, value) -> {
                String[] split = value.split(",");
                String buyer = split[1];
                String seller = split[2];
                String stock = split[3];
                int number = Integer.parseInt(split[4]);

                return List.of(
                    new KeyValue<>(buyer, BusinessTransaction.buy(buyer, stock, number)),
                    new KeyValue<>(seller, BusinessTransaction.sell(seller, stock, number)));
            }).to(OUTPUT, Produced.with(Serdes.String(), JsonSerdes.newSerdes(BusinessTransaction.class)));

        return builder;
    }
}
