package com.pszymczyk.app5


import com.pszymczyk.common.Message
import com.pszymczyk.common.MessageSerde
import com.pszymczyk.common.User
import com.pszymczyk.common.UserSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import spock.lang.Specification

class App5UnitSpec extends Specification {

    def "Should enrich messages with full user name"() {
        given:
            def properties = new Properties()
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.name)
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.name)
            StreamsBuilder streamsBuilder = App5.buildKafkaStreamsTopology()
            def topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), properties)
            def source = topologyTestDriver.createInputTopic(App5.APP_5_SOURCE,
                    Serdes.String().serializer(),
                    MessageSerde.newSerde().serializer())
            def state = topologyTestDriver.createInputTopic(App5.APP_5_STATE,
                    Serdes.String().serializer(),
                    UserSerde.newSerde().serializer())
            def sink = topologyTestDriver.createOutputTopic(App5.APP_5_SINK,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer())
        when:
            state.pipeInput("andrzej123", new User("Andrzej", "Golara"))
            state.pipeInput("pszymczyk", new User("Pawel", "Szymczyk"))
            source.pipeInput(new Message(System.currentTimeMillis(), "andrzej123", "pszymczyk", "Hello <user>! how are you?"))
            source.pipeInput(new Message(System.currentTimeMillis(), "andrzej123", "pszymczyk", "ping"))
            source.pipeInput(new Message(System.currentTimeMillis(), "ecom", "andrzej123", "Hi <user>, we have some speciall offer for you"))

        then:
            sink.readValuesToList() == ["Hello Pawel Szymczyk! how are you?",
                                        "ping",
                                        "Hi Andrzej Golara, we have some speciall offer for you"]
        and:
            sink.readKeyValuesToList() == []
        cleanup:
            topologyTestDriver.close()
    }
}
