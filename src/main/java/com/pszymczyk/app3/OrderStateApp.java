package com.pszymczyk.app3;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Set;

class OrderStateApp {

    static final String ORDERS = "orders";
    static final String ORDERS_STATE = "orders-state";

    static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "orders-app-main",
            builder,
            new NewTopic(ORDERS, 1, (short) 1),
            new NewTopic(ORDERS_STATE, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, OrderEvent> allOrdersEvents = builder.stream(ORDERS,
            Consumed.with(Serdes.String(), JsonSerdes.forA(OrderEvent.class)));

        KTable<String, OrderState> ordersStateTable = allOrdersEvents
            .filter((key, value) -> Set.of(ItemAdded.TYPE, ItemRemoved.TYPE).contains(value.getType()))
            .selectKey((key, value) -> value.getOrderId())
            .groupByKey()
            .aggregate(
                OrderState::create,
                (key, value, aggregate) ->
                    switch (value.getType()) {
                        case ItemAdded.TYPE -> aggregate.apply((ItemAdded) value);
                        case ItemRemoved.TYPE -> aggregate.apply((ItemRemoved) value);
                        default -> throw new IllegalStateException("Unknown event types should be filtered before!");
                    },
                Materialized.with(Serdes.String(), JsonSerdes.forA(OrderState.class)));

        ordersStateTable.toStream().to(ORDERS_STATE, Produced.with(Serdes.String(), JsonSerdes.forA(OrderState.class)));

        return builder;
    }
}