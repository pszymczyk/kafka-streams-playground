package com.pszymczyk.common;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinueProductionExceptionHandler extends DefaultProductionExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ContinueProductionExceptionHandler.class);

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        logger.error("Failed producing record {}", record, exception);
        return ProductionExceptionHandlerResponse.CONTINUE;
    }
}
