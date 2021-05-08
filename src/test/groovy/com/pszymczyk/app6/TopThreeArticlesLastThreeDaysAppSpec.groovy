package com.pszymczyk.app6

import com.jayway.jsonpath.JsonPath
import com.pszymczyk.IntegrationSpec
import com.pszymczyk.common.StreamsRunner
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import spock.lang.Shared

import java.time.Duration
import java.time.Instant

import static TopThreeArticlesLastThreeDaysApp.ARTICLES_VISITS
import static TopThreeArticlesLastThreeDaysApp.ARTICLES_VISITS_TOP_FIVE

class TopThreeArticlesLastThreeDaysAppSpec extends IntegrationSpec {

    @Shared
    KafkaStreams kafkaStreams

    def setupSpec() {
        kafkaStreams = new StreamsRunner().run(
                bootstrapServers,
                "top-five-articles-last-five-days-app-v1",
                TopThreeArticlesLastThreeDaysApp.buildKafkaStreamsTopology(),
                Map.of(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, ArticleEventTimeExtractor.class),
                new NewTopic(ARTICLES_VISITS, 1, (short) 1),
                new NewTopic(ARTICLES_VISITS_TOP_FIVE, 1, (short) 1))
    }

    def cleanupSpec() {
        kafkaStreams.close()
    }

    def "Should count top three articles ranking in last three days"() {
        given:
            def today = Instant.parse("2007-12-15T10:15:30.00Z").toEpochMilli()
            def yesterday = Instant.parse("2007-12-14T10:15:30.00Z").toEpochMilli()
            def dayBeforeYesterday = Instant.parse("2007-12-13T10:15:30.00Z").toEpochMilli()

            def monzoArticle = "Monzo and Innocent Drinks founders among high-profile execs floated for new Amazon TV show about startups"
            def spacexArticle = "Watch SpaceX launch 4 astronauts aboard a recycled Crew Dragon spaceship for NASA on Friday"
            def cloudKitchensArticle = "Travis Kalanick's stealth 5 billion startup, CloudKitchens, is Uber all over again, ruled by a 'temple of bros,' insiders say"
            def superscriptArticle = "Insurance startup Superscript used this pitch deck to raise 10 million in a funding round backed by Seedcamp"
            def academyAwardsArticle = "The 93rd Academy Awards will honor the best films of the year here's how to watch live this Sunday to see all the winners"

            kafkaConsumer.subscribe([ARTICLES_VISITS_TOP_FIVE])
        when: "simulate day before yesterday clicks"
            2.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$monzoArticle",                           
                            "time": $dayBeforeYesterday
                        }
                        """.toString()).get()
            }
            10.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$spacexArticle",                           
                            "time": $dayBeforeYesterday
                        }
                        """.toString()).get()
            }
            3.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$cloudKitchensArticle",                           
                            "time": $dayBeforeYesterday
                        }
                        """.toString()).get()
            }
            7.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$academyAwardsArticle",                           
                            "time": $dayBeforeYesterday
                        }
                        """.toString()).get()
            }
            1.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$superscriptArticle",                           
                            "time": $dayBeforeYesterday
                        }
                        """.toString()).get()
            }
        and: "simulate yesterday clicks"
            4.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$monzoArticle",                           
                            "time": $yesterday
                        }
                        """.toString()).get()
            }
            3.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$spacexArticle",                           
                            "time": $yesterday
                        }
                        """.toString()).get()
            }
            3.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$cloudKitchensArticle",                           
                            "time": $yesterday
                        }
                        """.toString()).get()
            }
            3.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$academyAwardsArticle",                           
                            "time": $yesterday
                        }
                        """.toString()).get()
            }
            2.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$superscriptArticle",                           
                            "time": $yesterday
                        }
                        """.toString()).get()
            }
        and: "simulate today clicks"
            15.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$monzoArticle",                           
                            "time": $today
                        }
                        """.toString()).get()
            }
            7.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$spacexArticle",                           
                            "time": $today
                        }
                        """.toString()).get()
            }
            2.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$cloudKitchensArticle",                           
                            "time": $today
                        }
                        """.toString()).get()
            }
            5.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$academyAwardsArticle",                           
                            "time": $today
                        }
                        """.toString()).get()
            }
            1.times {
                kafkaTemplate.send(ARTICLES_VISITS,
                        """
                        {
                            "articleTitle": "$superscriptArticle",                           
                            "time": $today
                        }
                        """.toString()).get()
            }
        and: "collect all events"
            Map<String, String> visitsRankingTable = [:]
            75.times {
                def consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500))
                logger.info("Received {} events", consumerRecords.size())
                consumerRecords.each {
                    visitsRankingTable.put(it.key(), it.value())
                }
            }
        then: "day before yesterday ranking"
            JsonPath.parse(visitsRankingTable.get("2007-12-11T00:00:00Z-2007-12-14T00:00:00Z-ranking")).with {
                assert it.read('$.gold.title', String) == spacexArticle
                assert it.read('$.gold.views', Long) == 10
                assert it.read('$.silver.title', String) == academyAwardsArticle
                assert it.read('$.silver.views', Long) == 7
                assert it.read('$.bronze.title', String) == cloudKitchensArticle
                assert it.read('$.bronze.views', Long) == 3
            }
        and: "yesterday's ranking"
            JsonPath.parse(visitsRankingTable.get("2007-12-12T00:00:00Z-2007-12-15T00:00:00Z-ranking")).with {
                assert it.read('$.gold.title', String) == spacexArticle
                assert it.read('$.gold.views', Long) == 13
                assert it.read('$.silver.title', String) == academyAwardsArticle
                assert it.read('$.silver.views', Long) == 10
                assert it.read('$.bronze.title', String) == monzoArticle
                assert it.read('$.bronze.views', Long) == 6
            }
        and: "today's ranking"
            JsonPath.parse(visitsRankingTable.get("2007-12-13T00:00:00Z-2007-12-16T00:00:00Z-ranking")).with {
                assert it.read('$.gold.title', String) == monzoArticle
                assert it.read('$.gold.views', Long) == 21
                assert it.read('$.silver.title', String) == spacexArticle
                assert it.read('$.silver.views', Long) == 20
                assert it.read('$.bronze.title', String) == academyAwardsArticle
                assert it.read('$.bronze.views', Long) == 15
            }

    }
}
