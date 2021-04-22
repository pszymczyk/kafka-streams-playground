package com.pszymczyk.app3

import com.jayway.jsonpath.JsonPath
import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static com.pszymczyk.app3.OrderStateApp.ORDERS
import static com.pszymczyk.app3.OrderStateApp.ORDERS_STATE


class OrdersStateAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "order-state-app-v1",
                OrderStateApp.buildKafkaStreamsTopology(),
                new NewTopic(ORDERS, 1, (short) 1),
                new NewTopic(ORDERS_STATE, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Should build orders state"() {
        given:
            def iphone = "iphone"
            def pampers = "pampers"
            def kindle = "kindle"
            def fairy = "fairy"

            def orderOne = UUID.randomUUID().toString()
            def orderTwo = UUID.randomUUID().toString()
            def orderThree = UUID.randomUUID().toString()
            kafkaConsumer.subscribe([ORDERS_STATE])
        when: "we send some not important it this case events"
            kafkaTemplate.send(ORDERS,
                    """
                        {
                            "orderId": "$orderOne",                           
                            "type": "${ItemAdded.TYPE}",
                            "item": "$iphone"
                        }
                        """.toString())
            kafkaTemplate.send(ORDERS,
                    """
                        {
                            "orderId": "$orderOne",                           
                            "type": "${ItemAdded.TYPE}",
                            "item": "$pampers"
                        }
                        """.toString())
            kafkaTemplate.send(ORDERS,
                    """
                        {
                            "orderId": "$orderOne",                           
                            "type": "${ItemAdded.TYPE}",
                            "item": "$pampers"
                        }
                        """.toString())
            kafkaTemplate.send(ORDERS,
                    """
                        {
                            "orderId": "$orderOne",                           
                            "type": "${ItemAdded.TYPE}",
                            "item": "$pampers"
                        }
                        """.toString())
            kafkaTemplate.send(ORDERS,
                    """
                        {
                            "orderId": "$orderTwo",                           
                            "type": "${ItemAdded.TYPE}",
                            "item": "$kindle"
                        }
                        """.toString())
            kafkaTemplate.send(ORDERS,
                    """
                        {
                            "orderId": "$orderThree",                           
                            "type": "${ItemAdded.TYPE}",
                            "item": "$fairy"
                        }
                        """.toString())
            kafkaTemplate.send(ORDERS,
                    """
                        {
                            "orderId": "$orderThree",                           
                            "type": "${ItemAdded.TYPE}",
                            "item": "$fairy"
                        }
                        """.toString())

            kafkaTemplate.send(ORDERS,
                    """
                        {
                            "orderId": "$orderThree",                           
                            "type": "${ItemAdded.TYPE}",
                            "item": "$fairy"
                        }
                        """.toString())

            kafkaTemplate.send(ORDERS,
                    """
                        {
                            "orderId": "$orderThree",                           
                            "type": "${ItemAdded.TYPE}",
                            "item": "$fairy"
                        }
                        """.toString())
        and: "collect all events"
            Map<String, String> orders = [:]
            10.times {
                def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
                logger.info("Received {} events", consumerRecords.size())
                consumerRecords.each {
                    orders.put(it.key(), it.value())
                }
            }
        then:
            JsonPath.parse(orders.get(orderOne)).read('$.items.iphone', Long) == 1
            JsonPath.parse(orders.get(orderOne)).read('$.items.pampers', Long) == 3
            JsonPath.parse(orders.get(orderTwo)).read('$.items.kindle', Long) == 1
            JsonPath.parse(orders.get(orderThree)).read('$.items.fairy', Long) == 4
    }
}
