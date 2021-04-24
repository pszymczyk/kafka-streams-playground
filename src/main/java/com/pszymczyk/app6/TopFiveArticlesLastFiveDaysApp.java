//package com.pszymczyk.app6;
//
//import com.pszymczyk.app5.ItemAdded;
//import com.pszymczyk.app5.ItemDetails;
//import com.pszymczyk.app5.ItemRemoved;
//import com.pszymczyk.app5.OrderEvent;
//import com.pszymczyk.app5.OrderItem;
//import com.pszymczyk.app5.OrderItemWithDetails;
//import com.pszymczyk.app5.OrderStateWithItemDetails;
//import com.pszymczyk.common.JsonSerdes;
//import com.pszymczyk.common.StreamsRunner;
//import org.apache.kafka.clients.admin.NewTopic;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.kstream.Consumed;
//import org.apache.kafka.streams.kstream.Grouped;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.KTable;
//import org.apache.kafka.streams.kstream.Materialized;
//import org.apache.kafka.streams.kstream.Produced;
//
//import java.util.Set;
//
//class TopFiveArticlesLastFiveDaysApp {
//
//    static final String ARTICLES_VISITS = "articles-visit";
//    static final String ARTICLES_VISITS_TOP_FIVE = "articles-visit-top-five";
//
//    static void main(String[] args) {
//        StreamsBuilder builder = buildKafkaStreamsTopology();
//        new StreamsRunner().run(
//            "localhost:9092",
//            "order-with-details-symmetric-app-main",
//            builder,
//            new NewTopic(ARTICLES_VISITS, 1, (short) 1),
//            new NewTopic(ARTICLES_VISITS_TOP_FIVE, 1, (short) 1));
//    }
//
//    static StreamsBuilder buildKafkaStreamsTopology() {
//        StreamsBuilder builder = new StreamsBuilder();
//
//        KStream<String, OrderEvent> allOrdersEvents = builder.stream(ARTICLES_VISITS,
//            Consumed.with(Serdes.String(), JsonSerdes.forA(OrderEvent.class)));
//
//        KTable<String, OrderItem> orderItems = allOrdersEvents
//            .filter((key, value) -> Set.of(ItemAdded.TYPE, ItemRemoved.TYPE).contains(value.getType()))
//            .groupBy((key, orderEvent) -> orderEvent.getOrderId(), Grouped.with(Serdes.String(), JsonSerdes.forA(OrderEvent.class)))
//            .aggregate(
//                OrderItem::create,
//                (key, value, aggregate) ->
//                    switch (value.getType()) {
//                        case ItemAdded.TYPE -> aggregate.apply((ItemAdded) value);
//                        case ItemRemoved.TYPE -> aggregate.apply((ItemRemoved) value);
//                        default -> throw new IllegalStateException("Unknown event types should be filtered before!");
//                    },
//                Materialized.with(Serdes.String(), JsonSerdes.forA(OrderItem.class)));
//
//        KTable<String, OrderItemWithDetails> orderItemsWithDetails = orderItems
//            .leftJoin(itemDetailsTable,
//                OrderItem::getItem,
//                OrderItemWithDetails::new,
//                Materialized.with(Serdes.String(), JsonSerdes.forA(OrderItemWithDetails.class)));
//
//        KTable<String, OrderStateWithItemDetails> ordersStateWithDetails = orderItemsWithDetails
//            .toStream()
//            .groupByKey()
//            .aggregate(OrderStateWithItemDetails::create,
//                (key, orderItemWithDetails, aggregate) -> aggregate.add(orderItemWithDetails),
//                Materialized.with(Serdes.String(), JsonSerdes.forA(OrderStateWithItemDetails.class)));
//
//
//        ordersStateWithDetails.toStream().to(ORDERS_WITH_DETAILS_STATE,
//            Produced.with(Serdes.String(), JsonSerdes.forA(OrderStateWithItemDetails.class)));
//        return builder;
//    }
//}