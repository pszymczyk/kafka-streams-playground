package com.pszymczyk.app1

import com.pszymczyk.app3.ItemAdded
import com.pszymczyk.app3.ItemRemoved
import com.pszymczyk.app3.OrderState
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import spock.lang.Specification

class MobileDevicesMarketShareUnitSpec extends Specification {


    TestEnvironment testEnvironment

    def setup() {
            testEnvironment = TestEnvironmentFactory.create()
    }

    def cleanup() {
        testEnvironment.close()
    }

    def "Should build orders state"() {
        when:
            testEnvironment.clicks.pipeInput("123#xxx#trololo")
            testEnvironment.clicks.pipeInput("123#xxx#trololo")
            testEnvironment.clicks.pipeInput("123#xxx#trololo")
            testEnvironment.clicks.pipeInput("123#xxx#trololo")
            testEnvironment.clicks.pipeInput("123#xxx#trololo")
        then:
            Map<String, String> map = testEnvironment.clicksCount
                    .readRecordsToList()
                    .collectEntries { [it.key(), it.value()] }
            map["trololo"] == "5"
    }

    private static class TestEnvironment {
        final TopologyTestDriver testDriver

        final TestInputTopic<String, String> clicks
        final TestOutputTopic<String, String> clicksCount

        TestEnvironment(TopologyTestDriver testDriver,
                        TestInputTopic<String, String> clicks,
                        TestOutputTopic<String, String> clicksCount) {
            this.testDriver = testDriver
            this.clicks = clicks
            this.clicksCount = clicksCount
        }

        void close() {
            testDriver.close()
        }
    }

    private static class TestEnvironmentFactory {

        static TestEnvironment create() {
            StreamsBuilder streamsBuilder = MobileDevicesMarketShareApp.buildKafkaStreamsTopology()
            def topology = streamsBuilder.build()
            def topologyTestDriver = new TopologyTestDriver(
                    topology,
                    testStreamProperties()
            )

            TestInputTopic<String, String> clicks = topologyTestDriver
                    .createInputTopic(
                            MobileDevicesMarketShareApp.CLICKS_TOPIC,
                            Serdes.String().serializer(),
                            Serdes.String().serializer())


            TestOutputTopic<String, String> clicksCount = topologyTestDriver
                    .createOutputTopic(
                            MobileDevicesMarketShareApp.CLICKS_COUNT,
                            Serdes.String().deserializer(),
                            Serdes.String().deserializer())

            return new TestEnvironment(
                    topologyTestDriver,
                    clicks,
                    clicksCount
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
