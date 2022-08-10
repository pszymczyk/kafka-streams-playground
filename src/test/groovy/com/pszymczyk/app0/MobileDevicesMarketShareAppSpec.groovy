package com.pszymczyk.app0;

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static com.pszymczyk.app0.MobileDevicesMarketShareApp.CLICKS_COUNT
import static com.pszymczyk.app0.MobileDevicesMarketShareApp.CLICKS_TOPIC

class MobileDevicesMarketShareAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "mobile-devices-market-share-main-v1",
                MobileDevicesMarketShareApp.buildKafkaStreamsTopology(),
                [:],
                new NewTopic(CLICKS_TOPIC, 1, (short) 1),
                new NewTopic(CLICKS_COUNT, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Count page clicks"() {
        given:
            kafkaConsumer.subscribe([CLICKS_COUNT])
        when: "send a lot of page clicks"
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#button123#firefox")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#button123#chrome")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#button123#IE")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#button123#edge")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#panel123#firefox")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#panel123#firefox")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#panel123#edge")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#panel123#firefox")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#link982721#chrome")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#link982721#chrome")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#link982721#edge")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#link982721#edge")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#link982721#edge")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#firefox")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#chrome")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#chrome")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#chrome")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#chrome")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#chrome")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#firefox")
            produceMessage(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#firefox")
        and: "collect all events"
            Map<String, String> clicksCount = [:]
            20.times {
                def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
                logger.info("Received {} events", consumerRecords.size())
                consumerRecords.each {
                    logger.info("{}:{}", it.key(), it.value())
                    clicksCount.put(it.key(), it.value())
                }
            }
        then:
            clicksCount == ["firefox": "7",
                            "IE"     : "1",
                            "edge"   : "5",
                            "chrome" : "8"]
    }
}
