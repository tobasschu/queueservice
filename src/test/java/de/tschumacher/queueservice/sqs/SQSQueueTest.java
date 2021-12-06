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
package de.tschumacher.queueservice.sqs;

import static de.tschumacher.queueservice.DataCreator.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class SQSQueueTest {
    private SQSQueue sqsQueue;
    private AmazonSQSAsync sqs;
    private String queueName;
    private String queueUrl;

    @BeforeEach
    public void setUp() {
        this.queueName = "queueName1";
        this.sqs = Mockito.mock(AmazonSQSAsync.class);

        final GetQueueUrlResult getQueueUrlResult = createGetQueueUrlResult();
        this.queueUrl = getQueueUrlResult.getQueueUrl();
        when(this.sqs.getQueueUrl(this.queueName)).thenReturn(getQueueUrlResult);

        this.sqsQueue = new SQSQueueImpl(this.sqs, this.queueName);
    }

    @AfterEach
    public void shutDown() {
        verify(this.sqs).getQueueUrl(this.queueName);
        verifyNoMoreInteractions(this.sqs);
    }

    @Test
    public void sendMessageTest() {
        final String messageBody = "messageBody1";

        this.sqsQueue.sendMessage(messageBody);

        verifySendMessageRequest(messageBody, null);
    }

    @Test
    public void sendMessageWithDelaySecondsTest() {
        final String messageBody = "messageBody1";
        final Integer delaySeconds = 5;

        this.sqsQueue.sendMessage(messageBody, delaySeconds);

        verifySendMessageRequest(messageBody, delaySeconds);
    }

    @Test
    public void changeMessageVisibilityTest() {
        final String receiptHandle = "receiptHandle1";
        final Integer retrySeconds = 5;

        this.sqsQueue.changeMessageVisibility(receiptHandle, retrySeconds);

        verify(this.sqs).changeMessageVisibility(this.queueUrl, receiptHandle, retrySeconds);
    }

    @Test
    public void deleteMessageTest() {
        final String receiptHandle = "receiptHandle1";

        this.sqsQueue.deleteMessage(receiptHandle);

        verify(this.sqs).deleteMessage(this.queueUrl, receiptHandle);
    }

    @Test
    public void getQueueArnTest() {
        final GetQueueAttributesResult getQueueAttributesResult = createGetQueueAttributesResult();
        when(this.sqs.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(getQueueAttributesResult);

        final String queueArn = this.sqsQueue.getQueueArn();

        assertEquals(getQueueAttributesResult.getAttributes().get("QueueArn"), queueArn);

        verify(this.sqs).getQueueAttributes(any(GetQueueAttributesRequest.class));
    }

    @Test
    public void receiveMessageTest() {
        final ReceiveMessageResult receiveMessageResult = createReceiveMessageResult();

        when(this.sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);

        final Message resultReceiveMessage = this.sqsQueue.receiveMessage();

        assertEquals(receiveMessageResult.getMessages().get(0), resultReceiveMessage);

        verify(this.sqs).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    public void receiveEmptyMessageTest() {
        when(this.sqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(new ReceiveMessageResult());

        final Message resultReceiveMessage = this.sqsQueue.receiveMessage();

        assertNull(resultReceiveMessage);

        verify(this.sqs).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    public void addSNSPermissionsTest() {
        final String topicArn = "topicArn1";
        final GetQueueAttributesResult getQueueAttributesResult = createGetQueueAttributesResult();

        when(this.sqs.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(getQueueAttributesResult);

        this.sqsQueue.addSNSPermissions(topicArn);

        verify(this.sqs).setQueueAttributes(any(SetQueueAttributesRequest.class));
        verify(this.sqs).getQueueAttributes(any(GetQueueAttributesRequest.class));
    }

    private void verifySendMessageRequest(final String messageBody, Integer delaySeconds) {
        final ArgumentCaptor<SendMessageRequest> sendMessageRequestCaptor = ArgumentCaptor.forClass(
            SendMessageRequest.class
        );
        verify(this.sqs).sendMessageAsync(sendMessageRequestCaptor.capture());

        assertEquals(messageBody, sendMessageRequestCaptor.getValue().getMessageBody());
        assertEquals(this.queueUrl, sendMessageRequestCaptor.getValue().getQueueUrl());
        assertEquals(delaySeconds, sendMessageRequestCaptor.getValue().getDelaySeconds());
    }
}
