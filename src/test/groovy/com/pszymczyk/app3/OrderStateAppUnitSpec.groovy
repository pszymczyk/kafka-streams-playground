package com.pszymczyk.app3

import com.pszymczyk.common.JsonSerdes
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import spock.lang.Specification

class OrderStateAppUnitSpec extends Specification {

    def "Should build orders state"() {
        given:
            def iphone = "iphone"
            def pampers = "pampers"
            def kindle = "kindle"
            def fairy = "fairy"
            def orderOne = UUID.randomUUID().toString()
            def orderTwo = UUID.randomUUID().toString()
            def orderThree = UUID.randomUUID().toString()

            def orderEvents = [
                    new ItemAdded(orderOne, iphone),
                    new ItemAdded(orderOne, pampers),
                    new ItemAdded(orderOne, kindle),
                    new ItemAdded(orderOne, kindle),
                    new ItemAdded(orderOne, kindle),
                    new ItemAdded(orderTwo, fairy),
                    new ItemAdded(orderTwo, fairy),
                    new ItemAdded(orderTwo, fairy),
                    new ItemAdded(orderThree, pampers),
                    new ItemAdded(orderThree, pampers),
                    new ItemAdded(orderThree, pampers),
                    new ItemAdded(orderThree, pampers),
                    new ItemRemoved(orderThree, pampers),
                    new ItemRemoved(orderThree, pampers),
                    new ItemAdded(orderThree, pampers),
                    new ItemRemoved(orderOne, kindle),
                    new ItemRemoved(orderOne, kindle),
                    new ItemRemoved(orderTwo, fairy),
            ]

            TestEnvironment testEnvironment = TestEnvironmentFactory.create()
        when:
            orderEvents.each { testEnvironment.orders.pipeInput(it) }
        then:
            Map<String, OrderState> map = testEnvironment.ordersState
                    .readRecordsToList()
                    .collectEntries { [it.key(), it.value()] }
            map[orderOne] == new OrderState(orderOne, [iphone: 1L, pampers: 1L, kindle: 1L])
            map[orderTwo] == new OrderState(orderTwo, [fairy: 2L])
            map[orderThree] == new OrderState(orderThree, [pampers: 3L])
        and:
            testEnvironment.close()
    }

    private static class TestEnvironment {
        final TopologyTestDriver testDriver

        final TestInputTopic<String, OrderEvent> orders
        final TestOutputTopic<String, OrderState> ordersState

        TestEnvironment(TopologyTestDriver testDriver,
                        TestInputTopic<String, OrderEvent> orders,
                        TestOutputTopic<String, OrderState> ordersState) {
            this.testDriver = testDriver
            this.orders = orders
            this.ordersState = ordersState
        }

        void close() {
            testDriver.close()
        }
    }

    private static class TestEnvironmentFactory {

        static TestEnvironment create() {
            StreamsBuilder streamsBuilder = OrderStateApp.buildKafkaStreamsTopology()
            def topology = streamsBuilder.build()
            def topologyTestDriver = new TopologyTestDriver(
                    topology,
                    testStreamProperties()
            )

            TestInputTopic<String, OrderEvent> orders = topologyTestDriver
                    .createInputTopic(
                            OrderStateApp.ORDERS,
                            Serdes.String().serializer(),
                            JsonSerdes.forA(OrderEvent.class).serializer())


            TestOutputTopic<String, OrderState> ordersState = topologyTestDriver
                    .createOutputTopic(
                            OrderStateApp.ORDERS_STATE,
                            Serdes.String().deserializer(),
                            JsonSerdes.forA(OrderState.class).deserializer())

            return new TestEnvironment(
                    topologyTestDriver,
                    orders,
                    ordersState
            )
        }

        private static Properties testStreamProperties() {
            HashMap<String, Object> configs = new HashMap<>();
            configs.put(StreamsConfig.APPLICATION_ID_CONFIG, this.class.simpleName);
            configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            return asProperties(configs)
        }

        private static Properties asProperties(Map<String, Object> properties) {
            Properties props = new Properties()
            props.putAll(properties);
            return props
        }
    }
}
