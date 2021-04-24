package com.pszymczyk.app6;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArticleEventTimeExtractor implements TimestampExtractor {

    private static final Logger log = LoggerFactory.getLogger(ArticleEventTimeExtractor.class);


    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        Object value = record.value();
        if (value instanceof ArticleVisited) {
            return timestampFromChargingEvent(record, (ArticleVisited) value);
        } else {
            throw new IllegalArgumentException("ArticleEventTimeExtractor can only handle ArticleVisited events");
        }
    }

    private long timestampFromChargingEvent(ConsumerRecord<Object, Object> record, ArticleVisited event) {
        Long timestamp = event.getTime();

        if (timestamp == null) {
            return onInvalidTimestamp(record, event);
        }

        return timestamp;
    }

    private long onInvalidTimestamp(ConsumerRecord<Object, Object> record, ArticleVisited event) {
        log.error("Charging event will be dropped because it has an invalid timestamp. Timestamp: {}, record: {}", event.getTime(), record);
        return -1;
    }

}