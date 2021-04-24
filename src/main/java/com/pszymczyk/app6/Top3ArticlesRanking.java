package com.pszymczyk.app6;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Top3ArticlesRanking {

    private final ArticleWithVisits gold;
    private final ArticleWithVisits silver;
    private final ArticleWithVisits bronze;


    @JsonCreator
    public Top3ArticlesRanking(
        @JsonProperty("gold") ArticleWithVisits gold,
        @JsonProperty("silver") ArticleWithVisits silver,
        @JsonProperty("bronze") ArticleWithVisits bronze) {
        this.gold = gold;
        this.silver = silver;
        this.bronze = bronze;
    }

    public static Top3ArticlesRanking empty() {
        return new Top3ArticlesRanking(null, null, null);
    }

    public ArticleWithVisits getGold() {
        return gold;
    }

    public ArticleWithVisits getSilver() {
        return silver;
    }

    public ArticleWithVisits getBronze() {
        return bronze;
    }
}
