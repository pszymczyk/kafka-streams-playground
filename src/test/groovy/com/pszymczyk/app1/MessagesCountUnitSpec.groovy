package com.pszymczyk.app1

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import spock.lang.Specification

class MessagesCountUnitSpec extends Specification {

    TestEnvironment testEnvironment

    def setup() {
        testEnvironment = TestEnvironmentFactory.create()
    }

    def cleanup() {
        testEnvironment.close()
    }

    def "Should build orders state"() {
        given:
        testEnvironment.messages.pipeInput("andrzej123#pszymczyk#Hello! how are you?")
        testEnvironment.messages.pipeInput("andrzej123#pszymczyk#Hello! how are you?")
        testEnvironment.messages.pipeInput("andrzej123#pszymczyk#Hello! how are you?")
        testEnvironment.messages.pipeInput("pszymczyk#andrzej123##Hello! how are you?")

        when:
        Map<String, String> messagesCount = testEnvironment.messagesCount
                .readRecordsToList()
                .collectEntries { [it.key(), it.value()] }

        then:
        messagesCount["pszymczyk"] == "3"
        messagesCount["andrzej123"] == "1"
    }

    private static class TestEnvironment {
        final TopologyTestDriver testDriver

        final TestInputTopic<String, String> messages
        final TestOutputTopic<String, String> messagesCount

        TestEnvironment(TopologyTestDriver testDriver,
                        TestInputTopic<String, String> messages,
                        TestOutputTopic<String, String> messagesCount) {
            this.testDriver = testDriver
            this.messages = messages
            this.messagesCount = messagesCount
        }

        void close() {
            testDriver.close()
        }
    }

    private static class TestEnvironmentFactory {

        static TestEnvironment create() {
            StreamsBuilder streamsBuilder = MessagesCountApp.buildKafkaStreamsTopology()
            def topology = streamsBuilder.build()
            def topologyTestDriver = new TopologyTestDriver(
                    topology,
                    testStreamProperties()
            )

            TestInputTopic<String, String> messages = topologyTestDriver
                    .createInputTopic(
                            MessagesCountApp.MESSAGES,
                            Serdes.String().serializer(),
                            Serdes.String().serializer())


            TestOutputTopic<String, String> messagesCount = topologyTestDriver
                    .createOutputTopic(
                            MessagesCountApp.MESSAGES_COUNT,
                            Serdes.String().deserializer(),
                            Serdes.String().deserializer())

            return new TestEnvironment(
                    topologyTestDriver,
                    messages,
                    messagesCount
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
