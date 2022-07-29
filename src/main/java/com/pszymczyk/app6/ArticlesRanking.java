package com.pszymczyk.app6;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ArticlesRanking {

    private final Map<String, Long> articlesRanking;

    @JsonCreator
    ArticlesRanking(@JsonProperty("articlesRanking") Map<String, Long> articlesRanking) {
        this.articlesRanking = articlesRanking;
    }

    public static ArticlesRanking create() {
        return new ArticlesRanking(new HashMap<>());
    }

    public Map<String, Long> getArticlesRanking() {
        return articlesRanking;
    }

    public ArticlesRanking apply(ArticleVisited articleVisited) {
        long visits = articlesRanking.getOrDefault(articleVisited.getArticleTitle(), 0L);
        articlesRanking.put(articleVisited.getArticleTitle(), ++visits);
        return this;
    }

    public Top3ArticlesRanking top3() {
        List<Map.Entry<String, Long>> collect = articlesRanking.entrySet()
                .stream()
                .sorted((o1, o2) -> -1 * o1.getValue().compareTo(o2.getValue()))
                .collect(Collectors.toList());

        if (collect.isEmpty()) {
            return Top3ArticlesRanking.empty();
        } else if (collect.size() < 2) {
            return new Top3ArticlesRanking(
                    new ArticleWithVisits(collect.get(0).getKey(), collect.get(0).getValue()),
                    null,
                    null
            );
        } else if (collect.size() < 3) {
            return new Top3ArticlesRanking(
                    new ArticleWithVisits(collect.get(0).getKey(), collect.get(0).getValue()),
                    new ArticleWithVisits(collect.get(1).getKey(), collect.get(1).getValue()),
                    null
            );
        } else {
            return new Top3ArticlesRanking(
                    new ArticleWithVisits(collect.get(0).getKey(), collect.get(0).getValue()),
                    new ArticleWithVisits(collect.get(1).getKey(), collect.get(1).getValue()),
                    new ArticleWithVisits(collect.get(2).getKey(), collect.get(2).getValue())
            );
        }
    }
}
