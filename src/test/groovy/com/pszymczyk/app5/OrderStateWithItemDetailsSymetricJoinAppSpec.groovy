package com.pszymczyk.app5

import com.jayway.jsonpath.JsonPath
import com.pszymczyk.IntegrationSpec
import com.pszymczyk.app4.OrderStateWithItemDetailsApp
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static com.pszymczyk.app5.OrderStateWithItemDetailsSymetricJoinApp.ITEMS_DETAILS
import static com.pszymczyk.app5.OrderStateWithItemDetailsSymetricJoinApp.ORDERS
import static com.pszymczyk.app5.OrderStateWithItemDetailsSymetricJoinApp.ORDERS_WITH_DETAILS_STATE

class OrderStateWithItemDetailsSymetricJoinAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "order-with-details-state-symetric-app-v1",
                OrderStateWithItemDetailsSymetricJoinApp.buildKafkaStreamsTopology(),
                new NewTopic(ITEMS_DETAILS, 1, (short) 1),
                new NewTopic(ORDERS, 1, (short) 1),
                new NewTopic(ORDERS_WITH_DETAILS_STATE, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Should build orders with details state and watch for item details changes"() {
        given:
            def iphone = "iphone"
            def orderOne = "order-one-key"
            kafkaConsumer.subscribe([ORDERS_WITH_DETAILS_STATE])
        when: "we send some item details"
            kafkaTemplate.send(ITEMS_DETAILS, iphone,
                    """
                        {
                            "name": "$iphone",                           
                            "price": "1000PLN",
                            "description": "iPhone 6s",
                            "category": "smartphones"
                        }
                        """.toString())
        and: "send some order events"
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
                            "item": "$iphone"
                        }
                        """.toString())
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
                            "type": "${ItemRemoved.TYPE}",
                            "item": "$iphone"
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
        then: "items on order are count correctly"
            JsonPath.parse(orders.get(orderOne)).read('$.items.iphone', Long) == 2
        and: "items have details"
            JsonPath.parse(orders.get(orderOne)).with {
                assert it.read('$.itemsDetails.iphone.description', String) == "iPhone 6s"
                assert it.read('$.itemsDetails.iphone.category', String) == "smartphones"
                assert it.read('$.itemsDetails.iphone.price', String) == "1000PLN"
            }
        when: "item price changed"
            kafkaTemplate.send(ITEMS_DETAILS, iphone,
                    """
                        {
                            "name": "$iphone",                           
                            "price": "999PLN",
                            "description": "iPhone 6s",
                            "category": "smartphones"
                        }
                        """.toString())
        and: "we collect all order events again"
            def yetAnotherConsumer = kafkaConsumer("${this.class.simpleName}- ${UUID.randomUUID().toString().substring(0,5)}")
            yetAnotherConsumer.subscribe([ORDERS_WITH_DETAILS_STATE])
            def ordersAfterItemDetailsChange= [:]
            10.times {
                def consumerRecords = yetAnotherConsumer.poll(Duration.ofMillis(500))
                logger.info("Received {} events", consumerRecords.size())
                consumerRecords.each {
                    ordersAfterItemDetailsChange.put(it.key(), it.value())
                }
            }
        then: "items price changed"
            JsonPath.parse(ordersAfterItemDetailsChange.get(orderOne)).with {
                assert it.read('$.itemsDetails.iphone.price', String) == "999PLN"
            }
    }
}
