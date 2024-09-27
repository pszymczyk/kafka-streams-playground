package com.pszymczyk.app3

import com.jayway.jsonpath.JsonPath
import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static App3.APP_3_SINK
import static App3.APP_3_SOURCE

class App3Spec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "app3-spec",
                App3.buildKafkaStreamsTopology(),
                [:],
                new NewTopic(APP_3_SOURCE, 1, (short) 1),
                new NewTopic(APP_3_SINK, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Should build inbox"() {
        given:
            produceMessage(APP_3_SOURCE, "1234#andrzej123#pszymczyk#Hello! how are you?")
            produceMessage(APP_3_SOURCE, "1235#andrzej123#pszymczyk#Hi! what is going on?")
            produceMessage(APP_3_SOURCE, "1236#telemarketing#andrzej123#We have a special discount for you!")
            produceMessage(APP_3_SOURCE, "1237#telemarketing#pszymczyk#Best wishes in Valentine's day!")

            kafkaConsumer.assign([new TopicPartition(APP_3_SINK, 0)])
        when: "collect all events"
            Map<String, String> inbox = [:]
            10.times {
                def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
                logger.info("Received {} events", consumerRecords.size())
                consumerRecords.each {
                    inbox.put(it.key(), new String(it.value()))
                }
            }
        then:
            JsonPath.parse(inbox.get("pszymczyk")).read('$.messages[0].message', String) == "Hello! how are you?"
            JsonPath.parse(inbox.get("pszymczyk")).read('$.messages[0].senderTime', Long) != null
            JsonPath.parse(inbox.get("pszymczyk")).read('$.messages[0].inboxTime', Long) != null
            JsonPath.parse(inbox.get("pszymczyk")).read('$.messages[1].message', String) == "Hi! what is going on?"
            JsonPath.parse(inbox.get("pszymczyk")).read('$.messages[1].senderTime', Long) != null
            JsonPath.parse(inbox.get("pszymczyk")).read('$.messages[1].inboxTime', Long) != null
            JsonPath.parse(inbox.get("pszymczyk")).read('$.messages[2].message', String) == "Best wishes in Valentine's day!"
            JsonPath.parse(inbox.get("pszymczyk")).read('$.messages[2].senderTime', Long) != null
            JsonPath.parse(inbox.get("pszymczyk")).read('$.messages[2].inboxTime', Long) != null
            JsonPath.parse(inbox.get("andrzej123")).read('$.messages[0].message', String) == "We have a special discount for you!"
            JsonPath.parse(inbox.get("andrzej123")).read('$.messages[0].senderTime', Long) != null
            JsonPath.parse(inbox.get("andrzej123")).read('$.messages[0].inboxTime', Long) != null
    }
}
