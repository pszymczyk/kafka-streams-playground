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
        produceMessage(MESSAGES, "1234#telemarketing#user-id-123#Hello <user>, Here is some extra deal for you!")
        produceMessage(MESSAGES, "1235#telemarketing#user-id-456#Hi <user>, Your order is completed.")
        produceMessage(MESSAGES, "1236#telemarketing#user-id-789#Alo <user>, All the best in valentine's day.")

        and: "collect all events"
        List<String> messages = []
        5.times {
            def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
            logger.info("Received {} events", consumerRecords.size())
            consumerRecords.each {
                messages.add(new String(it.value()))
            }
        }

        then: "messages have user friendly format"
        messages == ["Hello Pawel Szymczyk, Here is some extra deal for you!",
                     "Hi Jan Kowalski, Your order is completed.",
                     "Alo Anna Hiacynta, All the best in valentine's day."]
    }
}
