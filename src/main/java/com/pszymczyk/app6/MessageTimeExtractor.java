package com.pszymczyk.app6;

import com.pszymczyk.common.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageTimeExtractor implements TimestampExtractor {

    private static final Logger log = LoggerFactory.getLogger(MessageTimeExtractor.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        Object value = record.value();
        if (value instanceof Message) {
            return timestampFromChargingEvent(record, (Message) value);
        } else {
            throw new IllegalArgumentException("ArticleEventTimeExtractor can only handle ArticleVisited events");
        }
    }

    private long timestampFromChargingEvent(ConsumerRecord<Object, Object> record, Message message) {
        Long timestamp = message.timestamp();

        if (timestamp == null) {
            return onInvalidTimestamp(record, message);
        }

        return timestamp;
    }

    private long onInvalidTimestamp(ConsumerRecord<Object, Object> record, Message message) {
        log.error("Charging event will be dropped because it has an invalid timestamp. Timestamp: {}, record: {}", message.timestamp(), record);
        return -1;
    }

}