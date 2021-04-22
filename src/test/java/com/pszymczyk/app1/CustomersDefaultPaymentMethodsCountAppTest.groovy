package com.pszymczyk.app1

import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static com.pszymczyk.app1.CustomersDefaultPaymentMethodsCountApp.CUSTOMER_PREFERENCES_TOPIC
import static com.pszymczyk.app1.CustomersDefaultPaymentMethodsCountApp.PAYMENT_METHODS_COUNT_TOPIC

class CustomersDefaultPaymentMethodsCountAppTest extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "CustomersDefaultPaymentMethodsCountApp_v1",
                CustomersDefaultPaymentMethodsCountApp.buildKafkaStreamsTopology(),
                new NewTopic("customer-preferences", 1, (short) 1),
                new NewTopic("user-id-to-default-payment-method", 1, (short) 1),
                new NewTopic("payment-methods-count", 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Count payment methods usage"() {
        given:
            def kazik = "kazik"
            def zbyszek = "zbyszek"
            def jadwiga = "jadwiga"
            def zenon = "zenon"
            def danuta = "danuta"
            kafkaConsumer.subscribe([PAYMENT_METHODS_COUNT_TOPIC])
        when: "we send some not important it this case events"
            kafkaTemplate.send(CUSTOMER_PREFERENCES_TOPIC,
                    """
                        {
                            "userId": "$kazik",                           
                            "type": "${PreferredLocationChanged.TYPE}",
                            "newLocation": "Polna 3, 12-932 Lublin"
                        }
                        """.toString())
        and: "send some PaymentMethodChanged events"
            kafkaTemplate.send(CUSTOMER_PREFERENCES_TOPIC,
                    """
                        {
                            "userId": "$kazik",                           
                            "type": "${PaymentMethodChanged.TYPE}",
                            "newPaymentMethod": "blik"
                        }
                        """.toString())
            kafkaTemplate.send(CUSTOMER_PREFERENCES_TOPIC,
                    """
                        {
                            "userId": "$zbyszek}",                           
                            "type": "${PaymentMethodChanged.TYPE}",
                            "newPaymentMethod": "blik"
                        }
                        """.toString())
            kafkaTemplate.send(CUSTOMER_PREFERENCES_TOPIC,
                    """
                        {
                            "userId": "$jadwiga",                           
                            "type": "${PaymentMethodChanged.TYPE}",
                            "newPaymentMethod": "blik"
                        }
                        """.toString())
            kafkaTemplate.send(CUSTOMER_PREFERENCES_TOPIC,
                    """
                        {
                            "userId": "$zenon",                           
                            "type": "${PaymentMethodChanged.TYPE}",
                            "newPaymentMethod": "blik"
                        }
                        """.toString())
            kafkaTemplate.send(CUSTOMER_PREFERENCES_TOPIC,
                    """
                        {
                            "userId": "$danuta",                           
                            "type": "${PaymentMethodChanged.TYPE}",
                            "newPaymentMethod": "blik"
                        }
                        """.toString())
        and: "some users change blik to other payment method"
            kafkaTemplate.send(CUSTOMER_PREFERENCES_TOPIC,
                    """
                        {
                            "userId": "$zenon",                           
                            "type": "${PaymentMethodChanged.TYPE}",
                            "newPaymentMethod": "cash"
                        }
                        """.toString())
            kafkaTemplate.send(CUSTOMER_PREFERENCES_TOPIC,
                    """
                        {
                            "userId": "$danuta",                           
                            "type": "${PaymentMethodChanged.TYPE}",
                            "newPaymentMethod": "bank_transfer"
                        }
                        """.toString())
            kafkaTemplate.send(CUSTOMER_PREFERENCES_TOPIC,
                    """
                        {
                            "userId": "$jadwiga",                           
                            "type": "${PaymentMethodChanged.TYPE}",
                            "newPaymentMethod": "bank_transfer"
                        }
                        """.toString())
        and: "collect all events"
            Map<String, String> paymentMethodCounts = [:]
            10.times {
                def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
                logger.info("Received {} events", consumerRecords.size())
                consumerRecords.each {
                    paymentMethodCounts.put(it.key(), it.value())
                }
            }

        then:
            paymentMethodCounts == ["blik"         : "2",
                                    "cash"         : "1",
                                    "bank_transfer": "2"]
    }
}
