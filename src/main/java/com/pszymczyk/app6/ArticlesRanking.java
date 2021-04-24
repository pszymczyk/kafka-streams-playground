package com.pszymczyk.app6;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

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

    public ArticlesRanking top5() {
        //TODO
        return this;
    }
}
