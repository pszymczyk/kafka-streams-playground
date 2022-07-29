package com.pszymczyk.app4;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;
import java.util.Set;

class OrderStateWithItemDetailsApp {

    static final String ORDERS = "orders";
    static final String ORDERS_WITH_DETAILS_STATE = "orders-with-details-state";
    static final String ITEMS_DETAILS = "items-details";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
                "localhost:9092",
                "order-with-details-app-main",
                builder,
                Map.of(),
                new NewTopic(ORDERS, 1, (short) 1),
                new NewTopic(ORDERS_WITH_DETAILS_STATE, 1, (short) 1),
                new NewTopic(ITEMS_DETAILS, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, ItemDetails> itemDetailsTable = builder.table(ITEMS_DETAILS,
                Consumed.with(Serdes.String(), JsonSerdes.forA(ItemDetails.class)));

        KStream<String, OrderEvent> allOrdersEvents = builder.stream(ORDERS,
                Consumed.with(Serdes.String(), JsonSerdes.forA(OrderEvent.class)));

        KTable<String, OrderStateWithItemDetails> ordersStateWithDetailsTable = allOrdersEvents
                .filter((key, value) -> Set.of(ItemAdded.TYPE, ItemRemoved.TYPE).contains(value.getType()))
                .selectKey(((key, value) -> value.getItem()))
                .join(itemDetailsTable,
                        (orderEvent, itemDetails) -> new EnrichedOrderEvent(itemDetails, orderEvent))
                .groupBy((key, enrichedOrderEvent) -> enrichedOrderEvent.getOrderEvent().getOrderId(),
                        Grouped.with(Serdes.String(), JsonSerdes.forA(EnrichedOrderEvent.class)))
                .aggregate(
                        OrderStateWithItemDetails::create,
                        (key, value, aggregate) ->
                                switch (value.getOrderEvent().getType()) {
                                    case ItemAdded.TYPE ->
                                            aggregate.apply((ItemAdded) value.getOrderEvent(), value.getItemDetails());
                                    case ItemRemoved.TYPE -> aggregate.apply((ItemRemoved) value.getOrderEvent());
                                    default ->
                                            throw new IllegalStateException("Unknown event types should be filtered before!");
                                },
                        Materialized.with(Serdes.String(), JsonSerdes.forA(OrderStateWithItemDetails.class)));

        ordersStateWithDetailsTable.toStream().to(ORDERS_WITH_DETAILS_STATE,
                Produced.with(Serdes.String(), JsonSerdes.forA(OrderStateWithItemDetails.class)));

        return builder;
    }
}