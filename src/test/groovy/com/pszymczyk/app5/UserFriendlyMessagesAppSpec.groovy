package com.pszymczyk.app5

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static com.pszymczyk.app5.UserFriendlyMessagesApp.*

class UserFriendlyMessagesAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "app5-user-friendly-messages-spec",
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
        produceMessage(USERS, "user-id-123", "Pawel#Szymczyk")
        produceMessage(USERS, "user-id-456", "Jan#Kowalski")
        produceMessage(USERS, "user-id-789", "Anna#Hiacynta")

        and: "send some messages"
        produceMessage(MESSAGES, "user-id-123", "1234#telemarketing#user-id-123#Hello <user>, Here is some extra deal for you!")
        produceMessage(MESSAGES, "user-id-456", "1235#telemarketing#user-id-456#Hi <user>, Your order is completed.")
        produceMessage(MESSAGES, "user-id-789", "1236#telemarketing#user-id-789#Alo <user>, All the best in valentine's day.")

        and: "collect all events"
        Map<String, String> messages = [:]
        10.times {
            def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
            logger.info("Received {} events", consumerRecords.size())
            consumerRecords.each {
                messages.put(it.key(), it.value())
            }
        }

        then: "messages have user friendly format"
        messages == ["user-id-123": "Hello Pawel Szymczyk, Here is some extra deal for you!",
                     "user-id-456": "Hi Jan Kowalski, Your order is completed.",
                     "user-id-789": "Alo Anna Hiacynta, All the best in valentine's day."]

        when: "user details changed"
        produceMessage(USERS, "user-id-789", "Anna#Kowalska")

        and: "we collect all order events again"
        kafkaConsumer.seekToBeginning([new TopicPartition(USER_FRIENDLY_MESSAGES, 0)])
        def messagesAfterUserDetailsChanged = [:]
        10.times {
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
