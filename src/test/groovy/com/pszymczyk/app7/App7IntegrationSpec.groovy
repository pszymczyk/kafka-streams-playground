package com.pszymczyk.app7

import com.jayway.jsonpath.JsonPath
import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import spock.lang.Shared

import java.time.Duration
import java.time.Instant

import static App7.APP_7_SOURCE
import static App7.APP_7_SINK

class App7IntegrationSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "app6-top-five-articles-last-five-days-app-v1",
                App7.buildKafkaStreamsTopology(),
                Map.of(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MessageTimeExtractor.class),
                new NewTopic(APP_7_SOURCE, 1, (short) 1),
                new NewTopic(APP_7_SINK, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Should aggregate three days long inbox"() {
        given:
            def today = Instant.parse("2007-12-15T10:15:30.00Z").toEpochMilli()
            def yesterday = Instant.parse("2007-12-14T10:15:30.00Z").toEpochMilli()
            def dayBeforeYesterday = Instant.parse("2007-12-13T10:15:30.00Z").toEpochMilli()

            produceMessage(APP_7_SOURCE, "$dayBeforeYesterday#andrzej123#pszymczyk#Hello! how are you?")
            produceMessage(APP_7_SOURCE, "$yesterday#romek123#pszymczyk#Hello! how are you?")
            produceMessage(APP_7_SOURCE, "$today#andrzej123#pszymczyk#Hi! what is going on?")
            produceMessage(APP_7_SOURCE, "$dayBeforeYesterday#telemarketing#andrzej123#We have a special discount for you!")
            produceMessage(APP_7_SOURCE, "$yesterday#mango#andrzej123#Best wishes in Valentine's day!")
            produceMessage(APP_7_SOURCE, "$today#telemarketing#andrzej123#Best wishes in Valentine's day!")

            kafkaConsumer.assign([new TopicPartition(APP_7_SINK, 0)])

        when: "collect all events"
            Map<String, String> inboxTable = [:]
            15.times {
                def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
                logger.info("Received {} events", consumerRecords.size())
                consumerRecords.each {
                    logger.info("Received {}:{}", it.key(), it.value())
                    inboxTable.put(it.key(), new String(it.value()))
                }
            }
        then: "pszymczyk inbox"
            JsonPath.parse(inboxTable.get("2007-12-13T00:00:00Z-2007-12-16T00:00:00Z-pszymczyk")).with {
                assert it.read('$.messages', List.class).size() == 3
                assert it.read('$.messages.[0:9].sender', List.class) == ["andrzej123", "romek123", "andrzej123"]
            }
        and: "andrzej123 inbox"
            JsonPath.parse(inboxTable.get("2007-12-13T00:00:00Z-2007-12-16T00:00:00Z-andrzej123")).with {
                assert it.read('$.messages', List.class).size() == 3
                assert it.read('$.messages.[0:9].sender', List.class) == ["telemarketing", "mango", "telemarketing"]
            }
    }
}
