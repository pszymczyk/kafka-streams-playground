package com.pszymczyk.app5;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;
import java.util.Set;

class OrderStateWithItemDetailsSymmetricJoinApp {

    static final String ORDERS = "symmetric_orders";
    static final String ORDERS_WITH_DETAILS_STATE = "symmetric_orders-with-details-state";
    static final String ITEMS_DETAILS = "symmetric_items-details";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "order-with-details-symmetric-app-main",
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

        KTable<String, OrderItem> orderItems = allOrdersEvents
            .filter((key, value) -> Set.of(ItemAdded.TYPE, ItemRemoved.TYPE).contains(value.getType()))
            .groupBy((key, orderEvent) -> String.format("%s-%s", orderEvent.getOrderId(),  orderEvent.getItem()),
                Grouped.with(Serdes.String(), JsonSerdes.forA(OrderEvent.class)))
            .aggregate(
                OrderItem::create,
                (key, value, aggregate) ->
                    switch (value.getType()) {
                        case ItemAdded.TYPE -> aggregate.apply((ItemAdded) value);
                        case ItemRemoved.TYPE -> aggregate.apply((ItemRemoved) value);
                        default -> throw new IllegalStateException("Unknown event types should be filtered before!");
                    },
                Materialized.with(Serdes.String(), JsonSerdes.forA(OrderItem.class)));

        KTable<String, OrderItemWithDetails> orderItemsWithDetails = orderItems
            .join(itemDetailsTable,
                OrderItem::getItem,
                OrderItemWithDetails::new,
                Materialized.with(Serdes.String(), JsonSerdes.forA(OrderItemWithDetails.class)));

        KTable<String, OrderStateWithItemDetails> ordersStateWithDetails = orderItemsWithDetails
            .toStream()
            .groupBy((key, orderItemWithDetails) -> orderItemWithDetails.getOrderItem().getOrderId())
            .aggregate(OrderStateWithItemDetails::create,
                (key, orderItemWithDetails, aggregate) -> aggregate.add(orderItemWithDetails),
                Materialized.with(Serdes.String(), JsonSerdes.forA(OrderStateWithItemDetails.class)));


        ordersStateWithDetails.toStream().to(ORDERS_WITH_DETAILS_STATE,
            Produced.with(Serdes.String(), JsonSerdes.forA(OrderStateWithItemDetails.class)));
        return builder;
    }
}