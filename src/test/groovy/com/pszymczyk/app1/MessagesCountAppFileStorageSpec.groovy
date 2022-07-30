package com.pszymczyk.app1

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import spock.lang.Shared

import static MessagesCountApp.MESSAGES_COUNT
import static MessagesCountApp.MESSAGES
import static com.pszymczyk.app1.MobileDevicesMarketShareAppFileStorage.MOBILE_DEVICES_MARKET_SHARE_STORE
import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore

class MessagesCountAppFileStorageSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "mobile-devices-market-share-file-storage-spec",
                MobileDevicesMarketShareAppFileStorage.buildKafkaStreamsTopology(),
                [:],
                new NewTopic(MESSAGES, 1, (short) 1),
                new NewTopic(MESSAGES_COUNT, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Count page clicks"() {
        given: "send a lot of page clicks"
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#button123#firefox")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#button123#chrome")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#button123#IE")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#button123#edge")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#panel123#firefox")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#panel123#firefox")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#panel123#edge")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#panel123#firefox")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#link982721#chrome")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#link982721#chrome")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#link982721#edge")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#link982721#edge")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#link982721#edge")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#tree12#firefox")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#tree12#chrome")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#tree12#chrome")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#tree12#chrome")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#tree12#chrome")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#tree12#chrome")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#tree12#firefox")
        sendToKafka(MESSAGES, "${System.currentTimeMillis()}#tree12#firefox")
        when: "collect all events"
        Map<String, String> clicksCount = [:]
        5.times {
            try {
                ReadOnlyKeyValueStore<String, String> store = kafkaStreams.store(fromNameAndType(MOBILE_DEVICES_MARKET_SHARE_STORE, keyValueStore()))
                logger.info("Received {} events", store.approximateNumEntries())
                store.all().each {
                    logger.info("{}:{}", it.key, it.value)
                    clicksCount.put(it.key, it.value)
                }
            } catch (InvalidStateStoreException ignored) {
            } finally {
                println("Waiting...")
                sleep(500)
            }
        }
        then:
        clicksCount == ["firefox": 7,
                        "IE"     : 1,
                        "edge"   : 5,
                        "chrome" : 8]
    }
}
