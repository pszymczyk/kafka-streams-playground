package com.pszymczyk.app1

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.nio.ByteBuffer
import java.time.Duration

import static MessagesCountApp.MESSAGES
import static MessagesCountApp.MESSAGES_COUNT

class MessagesCountAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "app1-messages-count-spec",
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
        produceMessage(MESSAGES, "1234#andrzej123#pszymczyk#Hello! how are you?")
        produceMessage(MESSAGES, "1235#andrzej123#pszymczyk#Hi! what is going on?")
        produceMessage(MESSAGES, "1236#telemarketing#andrzej123#We have a special discount for you!")
        produceMessage(MESSAGES, "1237#telemarketing#pszymczyk#Best wishes in Valentine's day!")

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
