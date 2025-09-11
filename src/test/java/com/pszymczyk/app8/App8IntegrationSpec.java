package com.pszymczyk.app8;
//package com.pszymczyk.app8
//
//import com.jayway.jsonpath.JsonPath
//import com.pszymczyk.IntegrationSpec
//import com.pszymczyk.common.StreamsRunner
//import org.apache.kafka.clients.admin.NewTopic
//import org.apache.kafka.streams.KafkaStreams
//import spock.lang.Shared
//
//import java.time.Duration
//
//import static App8.SINK_TOPIC
//import static App8.SOURCE_TOPIC
//
//class com.pszymczyk.app8.App8IntegrationSpec extends IntegrationSpec {
//
//    @Shared
//    KafkaStreams kafkaStreams
//
//    def setupSpec() {
//        kafkaStreams = new StreamsRunner().run(
//                bootstrapServers,
//                "LoanApplicationProcess-app-v1",
//                App8.buildKafkaStreamsTopology(),
//                Map.of(),
//                new NewTopic(SOURCE_TOPIC, 1, (short) 1),
//                new NewTopic(SINK_TOPIC, 1, (short) 1))
//    }
//
//    def cleanupSpec() {
//        kafkaStreams.close()
//    }
//
//    def "Should count loans and limit given loan amount"() {
//        given:
//            def requester = "kazik"
//            kafkaConsumer.subscribe([SINK_TOPIC])
//        when: "send some loan applications"
//            produceMessage(SOURCE_TOPIC,
//                    """
//                        {
//                            "amount": 2000,
//                            "requester": "$requester"
//                        }
//                        """.toString())
//            produceMessage(SOURCE_TOPIC,
//                    """
//                        {
//                            "amount": 2000,
//                            "requester": "$requester"
//                        }
//                        """.toString())
//            produceMessage(SOURCE_TOPIC,
//                    """
//                        {
//                            "amount": 2000,
//                            "requester": "$requester"
//                        }
//                        """.toString())
//        and: "collect all events"
//            List<String> loanDecisions = []
//            7.times {
//                def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
//                logger.info("Received {} events", consumerRecords.size())
//                consumerRecords.each {
//                    loanDecisions.add(new String(it.value()))
//                }
//            }
//        then: "first full requested value"
//            JsonPath.parse(loanDecisions.first()).with {
//                assert it.read('$.amount', BigDecimal) == 2000.0
//                assert it.read('$.requester', String) == requester
//            }
//        and: "second 80% value"
//            JsonPath.parse(loanDecisions.get(1)).with {
//                assert it.read('$.amount', BigDecimal) == 1600.0
//                assert it.read('$.requester', String) == requester
//            }
//        and: "third 50% value"
//            JsonPath.parse(loanDecisions.get(2)).with {
//                assert it.read('$.amount', BigDecimal) == 1000.0
//                assert it.read('$.requester', String) == requester
//            }
//
//    }
//}
