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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;
import java.util.Set;

/**
 * WARNING!!!
 *
 * Symmetric joins between stream-stream , ktable-ktable or stream-ktable has strict requirements to work as expected.
 * In general we can say that the data we like to join needs to stick the same partitions.
 *
 * In our case to enrich OrderItem with ItemDetails OrderItem event and requested to join ItemDetails needs to be on the same partition which is
 * probably not possible to achieve on production environment.
 *
 * In details: topics that are joined need to be co-partitioned. That means that they need to have the same number of partitions.
 * The streaming application will fail if this is not the case. Producers to the topic also need to use the same partitioner although
 * that is something that cannot be verified by the streaming application as the partitioner is a property of the producer.
 * For example, you will not get any join results if you send some event “A” to partition 0 and the corresponding event to partition 1
 * even if both partitions are handled by the same instance of the streaming application.
 *
 * In order to do it, wee need to follow those points:
 * 1) Both topics should use the same key schema. For example if one topic uses userName as key and other userSurname
 * joining operation will work but most probably won't produce any meaningful output.
 * 2) Producer applications that are writing to joined topics should use the same partitioning strategy.
 * That way same keys will end up at the same partitions that are assigned to be joined.
 * 3) Both topics should use same message timestamp strategy(logAppendTime or CreteTime). This one is not a requirement
 * per say but should be considered for windowed joins if topics use different messageTimeStampTypes since messageTimeStamps
 * are used for determining relevant messages to join together and missing this can lead to hard to find bugs.
 *
 * See https://stackoverflow.com/questions/63209711/kafka-streams-joining-partitioned-topics
 *     https://www.confluent.io/blog/crossing-streams-joins-apache-kafka
 */
class OrderStateWithItemDetailsSymmetricJoinApp {

    static final String ORDERS = "symmetric_orders";
    static final String ORDERS_WITH_DETAILS_STATE = "symmetric_orders-with-details-state";
    static final String ITEMS_DETAILS = "symmetric_items-details";

    static void main(String[] args) {
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
            .groupBy((key, orderEvent) -> orderEvent.getOrderId(), Grouped.with(Serdes.String(), JsonSerdes.forA(OrderEvent.class)))
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
            .leftJoin(itemDetailsTable,
                OrderItem::getItem,
                OrderItemWithDetails::new,
                Materialized.with(Serdes.String(), JsonSerdes.forA(OrderItemWithDetails.class)));

        KTable<String, OrderStateWithItemDetails> ordersStateWithDetails = orderItemsWithDetails
            .toStream()
            .groupByKey()
            .aggregate(OrderStateWithItemDetails::create,
                (key, orderItemWithDetails, aggregate) -> aggregate.add(orderItemWithDetails),
                Materialized.with(Serdes.String(), JsonSerdes.forA(OrderStateWithItemDetails.class)));


        ordersStateWithDetails.toStream().to(ORDERS_WITH_DETAILS_STATE,
            Produced.with(Serdes.String(), JsonSerdes.forA(OrderStateWithItemDetails.class)));
        return builder;
    }
}