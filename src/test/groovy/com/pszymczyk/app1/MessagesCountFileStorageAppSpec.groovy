package com.pszymczyk.app1

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import spock.lang.Shared

import static MessagesCountFileStorageApp.*
import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore

class MessagesCountFileStorageAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "app1-fs-messages-count-spec",
                buildKafkaStreamsTopology(),
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

        and: "send a lot of messages"
        produceMessage(MESSAGES, "andrzej123#pszymczyk#Hello! how are you?")
        produceMessage(MESSAGES, "andrzej123#pszymczyk#Hi! what is going on?")
        produceMessage(MESSAGES, "telemarketing#andrzej123#We have a special discount for you!")
        produceMessage(MESSAGES, "telemarketing#pszymczyk#Best wishes in Valentine's day!")

        when: "collect all events"
        def messagesCount = [:]
        5.times {
            try {
                ReadOnlyKeyValueStore<String, String> store = kafkaStreams.store(fromNameAndType(STATE_STORE_NAME, keyValueStore()))
                logger.info("Received {} events", store.approximateNumEntries())
                store.all().each {
                    logger.info("{}:{}", it.key, it.value)
                    messagesCount.put(it.key, it.value)
                }
            } catch (InvalidStateStoreException ignored) {
            } finally {
                println("Waiting...")
                sleep(500)
            }
        }


        then:
        messagesCount == ["pszymczyk" : 3,
                          "andrzej123": 1]
    }
}
