package com.pszymczyk.app6;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class ArticleVisited {

    private final String articleId;
    private final String time;

    @JsonCreator
    ArticleVisited(
        @JsonProperty("articleId") String articleId,
        @JsonProperty("time") String time) {
        this.articleId = articleId;
        this.time = time;
    }

    String getArticleId() {
        return articleId;
    }

    String getTime() {
        return time;
    }
}
