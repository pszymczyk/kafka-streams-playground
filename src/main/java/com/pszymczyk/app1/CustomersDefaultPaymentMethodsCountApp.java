package com.pszymczyk.app1;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
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
            "favourite-color-app-1",
            builder,
            new NewTopic("customer-preferences", 1, (short) 1),
            new NewTopic("user-id-to-default-payment-method", 1, (short) 1),
            new NewTopic("payment-methods-count", 1, (short) 1));
    }

    public static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, CustomerPreferencesEvent> allCustomerPreferences = builder.stream(CUSTOMER_PREFERENCES_TOPIC,
            Consumed.with(Serdes.String(), JsonSerdes.forA(CustomerPreferencesEvent.class)));

        KStream<String, String> usersAndColours = allCustomerPreferences
            .filter((key, value) -> Objects.equals(value.getType(), PaymentMethodChanged.TYPE))
            .selectKey((key, value) -> value.getUserId())
            .mapValues(PaymentMethodChanged.class::cast)
            .mapValues(PaymentMethodChanged::getNewPaymentMethod)
            .filter((user, newPaymentMethod) -> Set.of("card", "cash", "blik", "bank_transfer").contains(newPaymentMethod.toLowerCase()));

        usersAndColours.to(USER_ID_TO_DEFAULT_PAYMENT_METHOD_TOPIC);

        KTable<String, String> usersAndDefaultPaymentMethods = builder.table("user-id-to-default-payment-method");

        KTable<String, Long> paymentMethodsCount = usersAndDefaultPaymentMethods
            .groupBy((user, colour) -> new KeyValue<>(colour, colour))
            .count(Named.as("PaymentMethodsCount"));

        paymentMethodsCount
            .toStream()
            .mapValues(longValue -> Long.toString(longValue))
            .to(PAYMENT_METHODS_COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return builder;
    }
}
