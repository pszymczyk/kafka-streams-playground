package com.pszymczyk.app1

import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static com.pszymczyk.app1.MobileDevicesMarketShareApp.CLICKS_COUNT
import static com.pszymczyk.app1.MobileDevicesMarketShareApp.CLICKS_TOPIC

class MobileDevicesMarketShareAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "mobile-devices-market-share-main-v1",
                MobileDevicesMarketShareApp.buildKafkaStreamsTopology(),
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
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#button123#firefox".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#button123#chrome".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#button123#IE".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#button123#edge".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#panel123#firefox".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#panel123#firefox".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#panel123#edge".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#panel123#firefox".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#link982721#chrome".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#link982721#chrome".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#link982721#edge".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#link982721#edge".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#link982721#edge".toString())
        and: "send some missing data"
            kafkaTemplate.send(CLICKS_TOPIC, "link982721#edge".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "link982721#edge".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#firefox".toString())
        and: "again send some nice clicks"
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#chrome".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#chrome".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#chrome".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#chrome".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#chrome".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#firefox".toString())
            kafkaTemplate.send(CLICKS_TOPIC, "${System.currentTimeMillis()}#tree12#firefox".toString())
        and: "collect all events"
            Map<String, String> clicksCount = [:]
            20.times {
                def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
                logger.info("Received {} events", consumerRecords.size())
                consumerRecords.each {
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
