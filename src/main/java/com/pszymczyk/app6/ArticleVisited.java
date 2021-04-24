package com.pszymczyk.app6;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class ArticleVisited {

    private final String articleTitle;
    private final Long time;

    @JsonCreator
    ArticleVisited(
        @JsonProperty("articleTitle") String articleTitle,
        @JsonProperty("time") Long time) {
        this.articleTitle = articleTitle;
        this.time = time;
    }

    String getArticleTitle() {
        return articleTitle;
    }

    Long getTime() {
        return time;
    }
}
