package com.pszymczyk.app7


import com.pszymczyk.common.Inbox
import com.pszymczyk.common.JsonSerdes
import com.pszymczyk.common.Message
import com.pszymczyk.common.MessageSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import spock.lang.Specification

import java.time.Instant

class App7UnitSpec extends Specification {

    static def today = Instant.parse("2007-12-15T10:15:30.00Z").toEpochMilli()
    static def yesterday = Instant.parse("2007-12-14T10:15:30.00Z").toEpochMilli()
    static def dayBeforeYesterday = Instant.parse("2007-12-13T10:15:30.00Z").toEpochMilli()

    def "Should aggregate three days long inbox"() {
        given:
            def properties = new Properties()
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.name)
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.name)
            properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MessageTimeExtractor.class)
            StreamsBuilder streamsBuilder = App7.buildKafkaStreamsTopology()
            def topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), properties)
            def source = topologyTestDriver.createInputTopic(App7.APP_7_SOURCE,
                    Serdes.String().serializer(),
                    MessageSerde.newSerde().serializer())
            def sink = topologyTestDriver.createOutputTopic(App7.APP_7_SINK,
                    Serdes.String().deserializer(),
                    JsonSerdes.newSerdes(Inbox).deserializer())
        when:
            source.pipeInput(new Message(today, "andrzej123", "pszymczyk", "Hello! how are you?"))
            source.pipeInput(new Message(today, "andrzej123", "pszymczyk", "trololo"))
            source.pipeInput(new Message(yesterday, "andrzej123", "pszymczyk", "ping"))
            source.pipeInput(new Message(dayBeforeYesterday, "andrzej123", "pszymczyk", "pong"))
        then:
            Map<String, Inbox> inboxMap = sink
                    .readRecordsToList()
                    .collectEntries { [it.key(), it.value()] }
        and:
            inboxMap["2007-12-13T00:00:00Z-2007-12-16T00:00:00Z-pszymczyk"].messages().size() == 4
            inboxMap["2007-12-14T00:00:00Z-2007-12-17T00:00:00Z-pszymczyk"].messages().size() == 3
            inboxMap["2007-12-15T00:00:00Z-2007-12-18T00:00:00Z-pszymczyk"].messages().size() == 2
        cleanup:
            topologyTestDriver.close()
    }
}
