package com.pszymczyk.app2

import com.pszymczyk.common.Message
import com.pszymczyk.common.MessageSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import spock.lang.Specification

class App2UnitSpec extends Specification {

    def "Should count messages sent to every user"() {
        given:
            def properties = new Properties()
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.name)
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.name)
            StreamsBuilder streamsBuilder = App2.buildKafkaStreamsTopology()
            def topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), properties)
            def source = topologyTestDriver.createInputTopic(App2.APP_2_SOURCE,
                    Serdes.String().serializer(),
                    MessageSerde.newSerde().serializer())
            def sink = topologyTestDriver.createOutputTopic(App2.APP_2_SINK,
                    Serdes.String().deserializer(),
                    Serdes.Long().deserializer())
        when:
            source.pipeInput(new Message(1234L, "andrzej123", "pszymczyk", "Hello! how are you?"))
            source.pipeInput(new Message(1235, "andrzej123", "pszymczyk", "Hello! how are you?"))
            source.pipeInput(new Message(1236, "andrzej123", "pszymczyk", "Hello! how are you?"))
            source.pipeInput(new Message(1237, "pszymczyk", "andrzej123", "Hello! how are you?"))

        then:
            Map<String, Long> messagesCount = sink
                    .readRecordsToList()
                    .collectEntries { [it.key(), it.value()] }

            messagesCount["pszymczyk"] == 3L
            messagesCount["andrzej123"] == 1L
        cleanup:
            topologyTestDriver.close()
    }
}
