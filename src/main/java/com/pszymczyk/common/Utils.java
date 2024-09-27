package com.pszymczyk.common;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class Utils {

    public static void failSometimes() {
        Random rand = new Random();
        int randomNum = rand.nextInt(0, 9);
        if (randomNum == 2) {
            throw new RuntimeException("Random number 2 = exception!");
        }
    }

    public static List<String> readLines(String path) {
        try {
            return Files.readAllLines(Path.of(Utils.class.getClassLoader().getResource(path).toURI()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static NewTopic createCompactedTopic(String topicName) {
        var compactedTopic = new NewTopic(topicName, 1, (short) 1);
        compactedTopic.configs(Map.of(
            TopicConfig.SEGMENT_MS_CONFIG, "" + Duration.ofMinutes(1).toMillis(),
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
        ));
        return compactedTopic;
    }
}
