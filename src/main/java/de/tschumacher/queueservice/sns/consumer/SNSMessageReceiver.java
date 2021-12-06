/*
 * Copyright 2018 Tobias Schumacher
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package de.tschumacher.queueservice.sns.consumer;

import com.amazonaws.services.sns.message.SnsMessageManager;
import com.amazonaws.services.sns.message.SnsNotification;
import com.amazonaws.services.sqs.model.Message;
import de.tschumacher.queueservice.message.MessageHandler;
import de.tschumacher.queueservice.message.SQSMessageFactory;
import de.tschumacher.queueservice.MessageReceiver;
import de.tschumacher.queueservice.sqs.SQSQueue;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SNSMessageReceiver<F> implements MessageReceiver<F> {
    private final MessageHandler<F> handler;
    private final SQSMessageFactory<F> factory;
    private final SnsMessageManager manager;

    public SNSMessageReceiver(final MessageHandler<F> handler, final SQSMessageFactory<F> factory) {
        this(handler, factory, new SnsMessageManager());
    }

    public void receiveMessages(final SQSQueue queue) {
        final List<Message> receiveMessages = queue.receiveMessages();
        for (Message receiveMessage : receiveMessages) {
            handleMessage(queue, receiveMessage);
        }
    }

    protected void handleMessage(final SQSQueue queue, final Message receiveMessage) {
        //TODO there might be a better way
        Message message = parseSnsMessage(receiveMessage);
        this.handler.receivedMessage(queue, this.factory.createSQSMessage(message));
        queue.deleteMessage(receiveMessage.getReceiptHandle());
    }

    private Message parseSnsMessage(Message receiveMessage) {
        final SnsNotification notification = createSnsNotification(receiveMessage.getBody());
        return new Message()
            .withMessageId(receiveMessage.getMessageId())
            .withBody(notification.getMessage())
            .withReceiptHandle(receiveMessage.getReceiptHandle())
            .addAttributesEntry("MessageGroupId", receiveMessage.getAttributes().get("MessageGroupId"));
    }

    private SnsNotification createSnsNotification(final String message) {
        final InputStream messageInputStream = new ByteArrayInputStream(message.getBytes(StandardCharsets.UTF_8));
        return (SnsNotification) this.manager.parseMessage(messageInputStream);
    }
}
