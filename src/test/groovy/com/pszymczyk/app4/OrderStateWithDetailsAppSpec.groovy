package com.pszymczyk.app4

import com.jayway.jsonpath.JsonPath
import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static com.pszymczyk.app4.OrderStateWithItemDetailsApp.ITEMS_DETAILS
import static com.pszymczyk.app4.OrderStateWithItemDetailsApp.ORDERS
import static com.pszymczyk.app4.OrderStateWithItemDetailsApp.ORDERS_WITH_DETAILS_STATE


class OrderStateWithDetailsAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "order-with-details-state-app-v1",
                OrderStateWithItemDetailsApp.buildKafkaStreamsTopology(),
                [:],
                new NewTopic(ITEMS_DETAILS, 1, (short) 1),
                new NewTopic(ORDERS, 1, (short) 1),
                new NewTopic(ORDERS_WITH_DETAILS_STATE, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Should build orders with details state"() {
        given:
            def iphone = "iphone"
            def pampers = "pampers"
            def kindle = "kindle"
            def fairy = "fairy"
            def orderOne = UUID.randomUUID().toString()
            def orderTwo = UUID.randomUUID().toString()
            def orderThree = UUID.randomUUID().toString()
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
            kafkaTemplate.send(ITEMS_DETAILS, pampers,
                    """
                        {
                            "name": "$pampers",                           
                            "price": "20PLN",
                            "description": "Pampers 2-3",
                            "category": "kids"
                        }
                        """.toString())
            kafkaTemplate.send(ITEMS_DETAILS, kindle,
                    """
                        {
                            "name": "$kindle",                           
                            "price": "400PLN",
                            "description": "Amazon Kindle reader",
                            "category": "eBook_readers"
                        }
                        """.toString())
            kafkaTemplate.send(ITEMS_DETAILS, fairy,
                    """
                        {
                            "name": "$fairy",                           
                            "price": "10PLN",
                            "description": "Fairy mint",
                            "category": "home"
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
        then: "items on order are count correctly"
            JsonPath.parse(orders.get(orderOne)).read('$.items.iphone', Long) == 1
            JsonPath.parse(orders.get(orderOne)).read('$.items.pampers', Long) == 3
            JsonPath.parse(orders.get(orderTwo)).read('$.items.kindle', Long) == 1
            JsonPath.parse(orders.get(orderThree)).read('$.items.fairy', Long) == 4
        and: "items have details"
            JsonPath.parse(orders.get(orderOne)).with {
                assert it.read('$.itemsDetails.iphone.description', String) == "iPhone 6s"
                assert it.read('$.itemsDetails.iphone.category', String) == "smartphones"
                assert it.read('$.itemsDetails.iphone.price', String) == "1000PLN"
                assert it.read('$.itemsDetails.pampers.description', String) == "Pampers 2-3"
                assert it.read('$.itemsDetails.pampers.category', String) == "kids"
                assert it.read('$.itemsDetails.pampers.price', String) == "20PLN"
            }
            JsonPath.parse(orders.get(orderTwo)).with {
                assert it.read('$.itemsDetails.kindle.description', String) == "Amazon Kindle reader"
                assert it.read('$.itemsDetails.kindle.category', String) == "eBook_readers"
                assert it.read('$.itemsDetails.kindle.price', String) == "400PLN"
            }
            JsonPath.parse(orders.get(orderThree)).with {
                assert it.read('$.itemsDetails.fairy.description', String) == "Fairy mint"
                assert it.read('$.itemsDetails.fairy.category', String) == "home"
                assert it.read('$.itemsDetails.fairy.price', String) == "10PLN"
            }
        when: "item category changed"
            kafkaTemplate.send(ITEMS_DETAILS, fairy,
                    """
                        {
                            "name": "$fairy",                           
                            "price": "10PLN",
                            "description": "Fairy mint",
                            "category": "grocery"
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
        then: "nothing happens in order with details view"
            orders == ordersAfterItemDetailsChange
    }
}
