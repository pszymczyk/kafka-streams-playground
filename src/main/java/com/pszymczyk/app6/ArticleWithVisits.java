package com.pszymczyk.app6;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ArticleWithVisits {
    private final String title;
    private final Long views;

    @JsonCreator
    public ArticleWithVisits(
        @JsonProperty("title") String title,
        @JsonProperty("views") Long views) {
        this.title = title;
        this.views = views;
    }

    public String getTitle() {
        return title;
    }

    public Long getViews() {
        return views;
    }
}
