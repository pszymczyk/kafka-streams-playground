package com.pszymczyk.app2

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static MessagesCountWithSerdeApp.MESSAGES
import static MessagesCountWithSerdeApp.MESSAGES_COUNT


class MessagesCountWithSerdeAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "messages-count-spec",
                MessagesCountWithSerdeApp.buildKafkaStreamsTopology(),
                [:],
                new NewTopic(MESSAGES, 1, (short) 1),
                new NewTopic(MESSAGES_COUNT, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Count messages per user"() {
        given:
        kafkaConsumer.subscribe([MESSAGES_COUNT])

        when: "send a lot of messages"
        produceMessage(MESSAGES, "123#andrzej123#pszymczyk#Hello! how are you?")
        produceMessage(MESSAGES, "124#andrzej123#pszymczyk#Hi! what is going on?")
        produceMessage(MESSAGES, "125#telemarketing#andrzej123#We have a special discount for you!")
        produceMessage(MESSAGES, "126#telemarketing#pszymczyk#Best wishes in Valentine's day!")

        and: "collect all events"
        Map<String, String> messagesCount = [:]
        10.times {
            def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
            logger.info("Received {} messages", consumerRecords.size())
            consumerRecords.each {
                logger.info("{}:{}", it.key(), it.value())
                messagesCount.put(it.key(), it.value())
            }
        }

        then:
        messagesCount == ["pszymczyk" : "3",
                          "andrzej123": "1"]
    }
}
