package com.pszymczyk.app8;

import com.pszymczyk.common.Message;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;


public class LoanApplicationProcess implements Transformer<String, Message, KeyValue<String, ReceivedMessage>> {

    private KeyValueStore<String, Inbox> inbox;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        inbox = context.getStateStore("users-loans-count");
        this.context = context;
    }

    @Override
    public KeyValue<String, ReceivedMessage> transform(String sender, Message message) {
        InboxMessage inboxMessage = inbox.get(sender).add(context.partition(), context.offset(), message);
        return new KeyValue<>(sender, new ReceivedMessage(inboxMessage.messageId()));
    }

    @Override
    public void close() {

    }
}
