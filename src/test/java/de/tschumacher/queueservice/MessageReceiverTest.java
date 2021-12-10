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
package de.tschumacher.queueservice;

import com.amazonaws.services.sqs.model.Message;
import de.tschumacher.queueservice.message.MessageHandler;
import de.tschumacher.queueservice.message.SQSMessage;
import de.tschumacher.queueservice.message.SQSMessageFactory;
import de.tschumacher.queueservice.message.TestDO;
import de.tschumacher.queueservice.MessageReceiver;
import de.tschumacher.queueservice.sqs.SQSQueue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class MessageReceiverTest {
    @Mock
    private SQSMessageFactory<TestDO> factory;

    @Mock
    private MessageHandler<TestDO> handler;

    @Mock
    private SQSQueue queue;

    private MessageReceiver<TestDO> sqsMessageReceiver;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        this.sqsMessageReceiver = new MessageReceiver<>(this.handler, this.factory);
    }

    @AfterEach
    public void shutDown() {
        Mockito.verifyNoMoreInteractions(this.queue);
        Mockito.verifyNoMoreInteractions(this.handler);
        Mockito.verifyNoMoreInteractions(this.factory);
    }

    @Test
    public void shouldReceiveEmptyListOfMessages() {
        Mockito.when(this.queue.receiveMessages()).thenReturn(Collections.emptyList());

        this.sqsMessageReceiver.receiveMessages(this.queue);

        Mockito.verify(this.queue).receiveMessages();
    }

    @Test
    public void shouldReceiveMultipleMessages() {
        List<Message> messages = Arrays.asList(
            new Message().withMessageId("messageId1"),
            new Message().withMessageId("messageId2")
        );

        final SQSMessage<TestDO> sqsMessage1 = SQSMessage
            .<TestDO>builder()
            .messageId("messageId1")
            .receiptHandle("receiptHandle1")
            .build();
        final SQSMessage<TestDO> sqsMessage2 = SQSMessage
            .<TestDO>builder()
            .messageId("messageId2")
            .receiptHandle("receiptHandle2")
            .build();

        Mockito.when(this.queue.receiveMessages()).thenReturn(messages);

        Mockito.when(this.factory.createSQSMessage(messages.get(0))).thenReturn(sqsMessage1);
        Mockito.when(this.factory.createSQSMessage(messages.get(1))).thenReturn(sqsMessage2);

        this.sqsMessageReceiver.receiveMessages(this.queue);

        Mockito.verify(this.queue).receiveMessages();

        Mockito.verify(this.factory).createSQSMessage(messages.get(0));
        Mockito.verify(this.factory).createSQSMessage(messages.get(1));

        Mockito.verify(this.handler).receivedMessage(this.queue, sqsMessage1);
        Mockito.verify(this.handler).receivedMessage(this.queue, sqsMessage2);

        Mockito.verify(this.queue).deleteMessage("receiptHandle1");
        Mockito.verify(this.queue).deleteMessage("receiptHandle2");
    }

    @Test
    public void shouldReceiveMessagesButFail() {
        List<Message> messages = Collections.singletonList(
            new Message().withMessageId("messageId1").withReceiptHandle("receiptHandle1")
        );

        final SQSMessage<TestDO> sqsMessage = SQSMessage.<TestDO>builder().messageId("messageId1").build();

        Mockito.when(this.queue.receiveMessages()).thenReturn(messages);
        Mockito.when(this.factory.createSQSMessage(messages.get(0))).thenReturn(sqsMessage);
        Mockito.doThrow(new RuntimeException("Error")).when(this.handler).receivedMessage(this.queue, sqsMessage);

        this.sqsMessageReceiver.receiveMessages(this.queue);

        Mockito.verify(this.queue).receiveMessages();
        Mockito.verify(this.factory).createSQSMessage(messages.get(0));
        Mockito.verify(this.handler).receivedMessage(this.queue, sqsMessage);
        Mockito.verify(this.queue).retryMessage("receiptHandle1");
    }
}
