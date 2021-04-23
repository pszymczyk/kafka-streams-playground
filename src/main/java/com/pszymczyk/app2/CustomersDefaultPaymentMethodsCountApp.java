package com.pszymczyk.app2;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Objects;
import java.util.Set;

public class CustomersDefaultPaymentMethodsCountApp {

    public static final String CUSTOMER_PREFERENCES_TOPIC = "customer-preferences";
    public static final String USER_ID_TO_DEFAULT_PAYMENT_METHOD_TOPIC = "user-id-to-default-payment-method";
    public static final String PAYMENT_METHODS_COUNT_TOPIC = "payment-methods-count";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "customers-default-payment-methods-count-app-main",
            builder,
            new NewTopic(CUSTOMER_PREFERENCES_TOPIC, 1, (short) 1),
            new NewTopic(USER_ID_TO_DEFAULT_PAYMENT_METHOD_TOPIC, 1, (short) 1),
            new NewTopic(PAYMENT_METHODS_COUNT_TOPIC, 1, (short) 1));
    }

    public static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, CustomerPreferencesEvent> allCustomerPreferences = builder.stream(CUSTOMER_PREFERENCES_TOPIC,
            Consumed.with(Serdes.String(), JsonSerdes.forA(CustomerPreferencesEvent.class)));

        KStream<String, String> usersDefaultPaymentsTopic = allCustomerPreferences
            .filter((key, value) -> Objects.equals(value.getType(), PaymentMethodChanged.TYPE))
            .selectKey((key, value) -> value.getUserId())
            .mapValues(PaymentMethodChanged.class::cast)
            .mapValues(PaymentMethodChanged::getNewPaymentMethod)
            .filter((user, newPaymentMethod) -> Set.of("card", "cash", "blik", "bank_transfer").contains(newPaymentMethod.toLowerCase()));

        usersDefaultPaymentsTopic.to(USER_ID_TO_DEFAULT_PAYMENT_METHOD_TOPIC);

        KTable<String, String> usersAndDefaultPaymentMethods = builder.table("user-id-to-default-payment-method");

        KTable<String, Long> paymentMethodsCount = usersAndDefaultPaymentMethods
            .groupBy((user, colour) -> new KeyValue<>(colour, colour))
            .count();

        paymentMethodsCount
            .toStream()
            .mapValues(longValue -> Long.toString(longValue))
            .to(PAYMENT_METHODS_COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return builder;
    }
}
