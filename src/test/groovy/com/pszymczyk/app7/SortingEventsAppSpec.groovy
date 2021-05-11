package com.pszymczyk.app7

import com.jayway.jsonpath.JsonPath
import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import spock.lang.Shared

import java.time.Duration

import static com.pszymczyk.app7.LoanApplicationProcessorApp.LOAN_APPLICATION_DECISIONS
import static com.pszymczyk.app7.LoanApplicationProcessorApp.LOAN_APPLICATION_REQUESTS

class SortingEventsAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "LoanApplicationProcess-app-v1",
                LoanApplicationProcessorApp.buildKafkaStreamsTopology(),
                Map.of(),
                new NewTopic(LOAN_APPLICATION_REQUESTS, 1, (short) 1),
                new NewTopic(LOAN_APPLICATION_DECISIONS, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Should count loans and limit given loan amount"() {
        given:
            def requester = "kazik"
            kafkaConsumer.subscribe([LOAN_APPLICATION_DECISIONS])
        when: "send some loan applications"
            kafkaTemplate.send(LOAN_APPLICATION_REQUESTS,
                    """
                        {
                            "amount": 2000,                           
                            "requester": "$requester"
                        }
                        """.toString()).get()
            kafkaTemplate.send(LOAN_APPLICATION_REQUESTS,
                    """
                        {
                            "amount": 2000,                           
                            "requester": "$requester"
                        }
                        """.toString()).get()
            kafkaTemplate.send(LOAN_APPLICATION_REQUESTS,
                    """
                        {
                            "amount": 2000,                           
                            "requester": "$requester"
                        }
                        """.toString()).get()
        and: "collect all events"
            List<String> loanDecisions = []
            15.times {
                def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
                logger.info("Received {} events", consumerRecords.size())
                consumerRecords.each {
                    loanDecisions.add(it.value())
                }
            }
        then: "first full requested value"
            JsonPath.parse(loanDecisions.first()).with {
                assert it.read('$.amount', BigDecimal) == 2000.0
                assert it.read('$.requester', String) == requester
            }
        and: "second 80% value"
            JsonPath.parse(loanDecisions.get(1)).with {
                assert it.read('$.amount', BigDecimal) == 1600.0
                assert it.read('$.requester', String) == requester
            }
        and: "third 50% value"
            JsonPath.parse(loanDecisions.get(2)).with {
                assert it.read('$.amount', BigDecimal) == 1000.0
                assert it.read('$.requester', String) == requester
            }

    }
}
