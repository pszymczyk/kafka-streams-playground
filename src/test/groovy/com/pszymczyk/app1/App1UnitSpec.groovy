package com.pszymczyk.app1

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import spock.lang.Specification

class App1UnitSpec extends Specification {


    def "Should split Hello World! string"() {
        given:
            def properties = new Properties()
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.name)
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.name)
            def topologyTestDriver = new TopologyTestDriver(App1.getTopology(), properties)

            TestInputTopic<String, String> source = topologyTestDriver
                    .createInputTopic(
                            "app1-source",
                            Serdes.String().serializer(),
                            Serdes.String().serializer())
            TestOutputTopic<String, String> sink = topologyTestDriver
                    .createOutputTopic(
                            "app1-sink",
                            Serdes.String().deserializer(),
                            Serdes.String().deserializer())
        when:
            source.pipeInput("Hello World!")
        then:
            sink.readValuesToList() == ["Hello", "World!"]
        cleanup:
            topologyTestDriver.close()
    }
}
