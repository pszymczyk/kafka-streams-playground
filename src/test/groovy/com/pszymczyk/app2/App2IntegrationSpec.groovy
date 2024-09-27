package com.pszymczyk.app2

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.nio.ByteBuffer
import java.time.Duration

import static App2.APP_2_SINK
import static App2.APP_2_SOURCE

class App2IntegrationSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "app2-spec",
                App2.buildKafkaStreamsTopology(),
                [:],
                new NewTopic(APP_2_SOURCE, 1, (short) 1),
                new NewTopic(APP_2_SINK, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Should count messages sent to every user"() {
        given:
            kafkaConsumer.assign([new TopicPartition(APP_2_SINK, 0)])

        when: "send a lot of messages"
            produceMessage(APP_2_SOURCE, "123#andrzej123#pszymczyk#Hello! how are you?")
            produceMessage(APP_2_SOURCE, "124#andrzej123#pszymczyk#Hi! what is going on?")
            produceMessage(APP_2_SOURCE, "125#telemarketing#andrzej123#We have a special discount for you!")
            produceMessage(APP_2_SOURCE, "126#telemarketing#pszymczyk#Best wishes in Valentine's day!")

        and: "collect all events"
            Map<String, Long> messagesCount = [:]
            10.times {
                def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
                logger.info("Received {} messages", consumerRecords.size())
                consumerRecords.each {
                    logger.info("{}:{}", it.key(), it.value())
                    messagesCount.put(it.key(), ByteBuffer.wrap(it.value()).getLong())
                }
            }

        then:
            messagesCount == ["pszymczyk" : 3L,
                              "andrzej123": 1L]
    }
}
