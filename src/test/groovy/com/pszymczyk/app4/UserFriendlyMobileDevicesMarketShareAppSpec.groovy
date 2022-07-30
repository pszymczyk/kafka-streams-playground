package com.pszymczyk.app4

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static com.pszymczyk.app4.UserFriendlyMessagesApp.*

class UserFriendlyMobileDevicesMarketShareAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "user-friendly-messages-spec",
                buildKafkaStreamsTopology(),
                [:],
                new NewTopic(USERS, 1, (short) 1),
                new NewTopic(MESSAGES, 1, (short) 1),
                new NewTopic(USER_FRIENDLY_MESSAGES, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Should build orders with details state"() {
        given:
        kafkaConsumer.subscribe([USER_FRIENDLY_MESSAGES])

        when: "send some user details"
        sendToKafka(USERS, "123", "Pawel#Szymczyk")
        sendToKafka(USERS, "456", "Jan#Kowalski")
        sendToKafka(USERS, "789", "Anna#Hiacynta")

        and: "send some messages"
        sendToKafka(MESSAGES, "123", "123#Hello <user>, Here is some extra deal for you!")
        sendToKafka(MESSAGES, "456", "456#Hi <user>, Your order is completed.")
        sendToKafka(MESSAGES, "789", "789#Alo <user>, All the best in valentine's day.")

        and: "collect all events"
        Map<String, String> messages = [:]
        5.times {
            def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
            logger.info("Received {} events", consumerRecords.size())
            consumerRecords.each {
                messages.put(it.key(), it.value())
            }
        }

        then: "messages have user friendly format"
        messages == ["123": "Hello Pawel Szymczyk, Here is some extra deal for you!",
                     "456": "Hi Jan Kowalski, Your order is completed.",
                     "789": "Alo Anna Hiacynta, All the best in valentine's day."]

        when: "user details changed"
        sendToKafka(USERS, "789", "Anna Kowalska")

        and: "we collect all order events again"
        kafkaConsumer.seekToBeginning([new TopicPartition(USER_FRIENDLY_MESSAGES, 0)])
        def messagesAfterUserDetailsChanged = [:]
        5.times {
            def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
            logger.info("Received {} events", consumerRecords.size())
            consumerRecords.each {
                messagesAfterUserDetailsChanged.put(it.key(), it.value())
            }
        }

        then: "nothing happened in already sent messages"
        messages == messagesAfterUserDetailsChanged
    }
}
