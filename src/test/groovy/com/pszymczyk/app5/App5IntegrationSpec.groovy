package com.pszymczyk.app5

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static App5.*

class App5IntegrationSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "app5-spec",
                buildKafkaStreamsTopology(),
                [:],
                new NewTopic(APP_5_STATE, 1, (short) 1),
                new NewTopic(APP_5_SOURCE, 1, (short) 1),
                new NewTopic(APP_5_SINK, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Should enrich messages with full user name"() {
        given:
            kafkaConsumer.assign([new TopicPartition(APP_5_SINK, 0)])

        when: "send some user details"
            produceMessage(APP_5_STATE, "user-id-123", "Pawel#Szymczyk")
            produceMessage(APP_5_STATE, "user-id-456", "Jan#Kowalski")
            produceMessage(APP_5_STATE, "user-id-789", "Anna#Hiacynta")

        and: "send some messages"
            produceMessage(APP_5_SOURCE, "1234#telemarketing#user-id-123#Hello <user>, Here is some extra deal for you!")
            produceMessage(APP_5_SOURCE, "1235#telemarketing#user-id-456#Hi <user>, Your order is completed.")
            produceMessage(APP_5_SOURCE, "1236#telemarketing#user-id-789#Alo <user>, All the best in valentine's day.")

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
