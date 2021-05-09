package com.pszymczyk.app6;

import com.pszymczyk.common.JsonSerdes;
import com.pszymczyk.common.StreamsRunner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.Map;

class TopThreeArticlesLastThreeDaysApp {

    static final String ARTICLES_VISITS = "articles-visit";
    static final String ARTICLES_VISITS_TOP_FIVE = "articles-visit-top-three";

    public static void main(String[] args) {
        StreamsBuilder builder = buildKafkaStreamsTopology();
        new StreamsRunner().run(
            "localhost:9092",
            "TopThreeArticlesLastThreeDaysApp-app-main",
            builder,
            Map.of(),
            new NewTopic(ARTICLES_VISITS, 1, (short) 1),
            new NewTopic(ARTICLES_VISITS_TOP_FIVE, 1, (short) 1));
    }

    static StreamsBuilder buildKafkaStreamsTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ArticleVisited> allArticleVisitedEvents = builder
            .stream(ARTICLES_VISITS, Consumed.with(Serdes.String(), JsonSerdes.forA(ArticleVisited.class)));

        KTable<Windowed<String>, ArticlesRanking> articlesRankingTable = allArticleVisitedEvents
            .groupBy((k, articleVisited) -> "ranking")
            .windowedBy(TimeWindows.of(Duration.ofDays(3)).advanceBy(Duration.ofDays(1)))
            .aggregate(
                ArticlesRanking::create,
                (key, articleVisited, articlesRanking) -> articlesRanking.apply(articleVisited),
                Materialized.with(Serdes.String(), JsonSerdes.forA(ArticlesRanking.class))
            );

        articlesRankingTable
            .mapValues(ArticlesRanking::top3)
            .toStream((key, value) -> String.format("%s-%s-%s", key.window().startTime(), key.window().endTime(), key.key()))
            .to(ARTICLES_VISITS_TOP_FIVE, Produced.with(Serdes.String(), JsonSerdes.forA(Top3ArticlesRanking.class)));

        return builder;
    }
}