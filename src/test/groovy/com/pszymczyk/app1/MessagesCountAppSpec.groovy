package com.pszymczyk.app1

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static MessagesCountApp.MESSAGES
import static MessagesCountApp.MESSAGES_COUNT

class MessagesCountAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "messages-count-spec",
                MessagesCountApp.buildKafkaStreamsTopology(),
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
        sendToKafka(MESSAGES, "pszymczyk#Hello! how are you?")
        sendToKafka(MESSAGES, "pszymczyk#Hi! what is going on?")
        sendToKafka(MESSAGES, "andrzej123#We have a special discount for you!")
        sendToKafka(MESSAGES, "pszymczyk#Best wishes in Valentine's day!")

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
