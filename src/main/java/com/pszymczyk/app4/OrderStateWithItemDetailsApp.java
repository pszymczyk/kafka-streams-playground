package com.pszymczyk.app4;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Set;

class OrderStateWithItemDetailsApp {

    static final String ORDERS = "orders";
    static final String ORDERS_WITH_DETAILS_STATE = "orders-with-details-state";
    static final String ITEMS_DETAILS = "items-details";

    static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "order-with-details-app-main",
            builder,
            new NewTopic(ORDERS, 1, (short) 1),
            new NewTopic(ORDERS_WITH_DETAILS_STATE, 1, (short) 1),
            new NewTopic(ITEMS_DETAILS, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, ItemDetails> itemDetailsTable = builder.globalTable(ITEMS_DETAILS,
            Consumed.with(Serdes.String(), JsonSerdes.forA(ItemDetails.class)));

        KStream<String, OrderEvent> allOrdersEvents = builder.stream(ORDERS,
            Consumed.with(Serdes.String(), JsonSerdes.forA(OrderEvent.class)));

        KTable<String, OrderStateWithItemDetails> ordersStateWithDetailsTable = allOrdersEvents
            .filter((key, value) -> Set.of(ItemAdded.TYPE, ItemRemoved.TYPE).contains(value.getType()))
            .join(itemDetailsTable,
                (key, orderEvent) -> orderEvent.getItem(),
                (orderEvent, itemDetails) -> new EnrichedOrderEvent(itemDetails, orderEvent))
            .groupBy((key, enrichedOrderEvent) -> enrichedOrderEvent.getOrderEvent().getOrderId(), Grouped.with(Serdes.String(),
                JsonSerdes.forA(EnrichedOrderEvent.class)))
            .aggregate(
                OrderStateWithItemDetails::create,
                (key, value, aggregate) ->
                    switch (value.getOrderEvent().getType()) {
                        case ItemAdded.TYPE -> aggregate.apply((ItemAdded) value.getOrderEvent(), value.getItemDetails());
                        case ItemRemoved.TYPE -> aggregate.apply((ItemRemoved) value.getOrderEvent());
                        default -> throw new IllegalStateException("Unknown event types should be filtered before!");
                    },
                Materialized.with(Serdes.String(), JsonSerdes.forA(OrderStateWithItemDetails.class)));

        ordersStateWithDetailsTable.toStream().to(ORDERS_WITH_DETAILS_STATE,
            Produced.with(Serdes.String(), JsonSerdes.forA(OrderStateWithItemDetails.class)));

        return builder;
    }
}