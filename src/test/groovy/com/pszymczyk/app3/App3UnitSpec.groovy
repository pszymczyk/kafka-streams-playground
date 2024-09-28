package com.pszymczyk.app3

import com.pszymczyk.app2.App2
import com.pszymczyk.common.Inbox
import com.pszymczyk.common.JsonSerdes
import com.pszymczyk.common.Message
import com.pszymczyk.common.MessageSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import spock.lang.Specification

class App3UnitSpec extends Specification {

    def "Should aggregate messages"() {
        given:
            def properties = new Properties()
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.name)
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.name)
            StreamsBuilder streamsBuilder = App3.buildKafkaStreamsTopology()
            def topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), properties)
            def source = topologyTestDriver.createInputTopic(App3.APP_3_SOURCE,
                    Serdes.String().serializer(),
                    MessageSerde.newSerde().serializer())
            def sink = topologyTestDriver.createOutputTopic(App3.APP_3_SINK,
                    Serdes.String().deserializer(),
                    JsonSerdes.newSerdes(Inbox).deserializer())
        when:
            source.pipeInput(new Message(System.currentTimeMillis(), "andrzej123", "pszymczyk", "Hello! how are you?"))
            source.pipeInput(new Message(System.currentTimeMillis(), "andrzej123", "pszymczyk", "trololo"))
            source.pipeInput(new Message(System.currentTimeMillis(), "andrzej123", "pszymczyk", "ping"))
            source.pipeInput(new Message(System.currentTimeMillis(), "pszymczyk", "andrzej123", "Hello!"))

        then:
            Map<String, Inbox> inboxMap = sink
                    .readRecordsToList()
                    .collectEntries { [it.key(), it.value()] }
        and:
            inboxMap["pszymczyk"].messages().size() == 3
            inboxMap["andrzej123"].messages().size() == 1
        and:
            with(inboxMap["pszymczyk"].messages()[0], {
                it.message() == "Hello! how are you?"
                it.sender() == "andrzej123"
                it.senderTime() < inboxMap["pszymczyk"].messages()[0].inboxTime()
            })
            with(inboxMap["pszymczyk"].messages()[1], {
                it.message() == "trololo"
                it.sender() == "andrzej123"
                it.senderTime() < inboxMap["pszymczyk"].messages()[1].inboxTime()
            })
            with(inboxMap["pszymczyk"].messages()[2], {
                it.message() == "ping"
                it.sender() == "andrzej123"
                it.senderTime() < inboxMap["pszymczyk"].messages()[2].inboxTime()
            })
        and:
            with(inboxMap["andrzej123"].messages()[0], {
                it.message() == "Hello!"
                it.sender() == "pszymczyk"
                it.senderTime() < inboxMap["andrzej123"].messages()[0].inboxTime()
            })
        cleanup:
            topologyTestDriver.close()
    }
}
