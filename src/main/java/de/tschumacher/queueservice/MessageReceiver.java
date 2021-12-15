/*
 * Copyright 2015 Tobias Schumacher
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
package de.tschumacher.queueservice;

import com.amazonaws.services.sqs.model.Message;
import de.tschumacher.queueservice.message.MessageHandler;
import de.tschumacher.queueservice.message.SQSMessage;
import de.tschumacher.queueservice.message.SQSMessageFactory;
import de.tschumacher.queueservice.sqs.SQSQueue;
import java.util.List;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
public class MessageReceiver<F> {
    private static final Logger logger = LoggerFactory.getLogger(MessageReceiver.class);

    private final MessageHandler<F> handler;
    private final SQSMessageFactory<F> factory;

    public void receiveMessages(final SQSQueue queue) {
        final List<Message> receiveMessages = queue.receiveMessages();
        for (Message receiveMessage : receiveMessages) {
            handleMessage(queue, receiveMessage);
        }
    }

    private void handleMessage(SQSQueue queue, Message receiveMessage) {
        try {
            SQSMessage<F> message = this.factory.createSQSMessage(receiveMessage);
            this.handler.receivedMessage(queue, message);
            queue.deleteMessage(message.getReceiptHandle());
        } catch (final Throwable e) {
            logger.error("Handling message failed for ID {}: {}", receiveMessage.getMessageId(), e.getMessage(), e);
            queue.retryMessage(receiveMessage.getReceiptHandle());
        }
    }
}
