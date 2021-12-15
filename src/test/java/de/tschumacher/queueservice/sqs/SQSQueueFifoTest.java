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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.auth.policy.conditions.ArnCondition;
import com.amazonaws.auth.policy.conditions.ConditionFactory;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.*;
import de.tschumacher.queueservice.message.SQSMessage;
import de.tschumacher.queueservice.message.TestDO;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SQSQueueFifoTest {
    private SQSQueue sqsQueue;

    @Mock
    private AmazonSQSAsync sqs;

    private final String queueUrl = "queueUrl1";
    private SQSQueueConfiguration configuration;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        configuration =
            SQSQueueConfiguration
                .builder()
                .queueName("queueName1.fifo")
                .secretKey("secretKey1")
                .accessKey("accessKey1")
                .build();

        when(this.sqs.getQueueUrl("queueName1.fifo")).thenReturn(new GetQueueUrlResult().withQueueUrl(queueUrl));

        this.sqsQueue = new SQSQueue(configuration, this.sqs);
    }

    @AfterEach
    public void shutDown() {
        verify(this.sqs).getQueueUrl("queueName1.fifo");
        verifyNoMoreInteractions(this.sqs);
    }

    @Test
    public void shouldCreateFifoQueue() {
        SQSQueueConfiguration configuration = SQSQueueConfiguration
            .builder()
            .queueName("queueName.fifo")
            .secretKey("secretKey1")
            .accessKey("accessKey1")
            .build();

        when(this.sqs.getQueueUrl("queueName.fifo")).thenThrow(new QueueDoesNotExistException("queueName.fifo"));
        when(
                this.sqs.createQueue(
                        new CreateQueueRequest()
                            .withQueueName("queueName.fifo")
                            .addAttributesEntry("FifoQueue", "true")
                            .addAttributesEntry("ContentBasedDeduplication", "true")
                    )
            )
            .thenReturn(new CreateQueueResult().withQueueUrl("queueUrl2"));

        this.sqsQueue = new SQSQueue(configuration, this.sqs);

        verify(this.sqs).getQueueUrl("queueName.fifo");
        verify(this.sqs)
            .createQueue(
                new CreateQueueRequest()
                    .withQueueName("queueName.fifo")
                    .addAttributesEntry("FifoQueue", "true")
                    .addAttributesEntry("ContentBasedDeduplication", "true")
            );
    }

    @Test
    public void shouldSendMessage() {
        final SQSMessage<TestDO> message = SQSMessage
            .<TestDO>builder()
            .plainContent("content1")
            .messageGroupId("messageGroupId1")
            .delay(5)
            .build();

        this.sqsQueue.sendMessage(message);

        SendMessageRequest expectedSendRequest = new SendMessageRequest()
            .withQueueUrl(queueUrl)
            .withMessageBody("content1")
            .withDelaySeconds(5)
            .withMessageGroupId("messageGroupId1");

        verify(this.sqs).sendMessageAsync(eq(expectedSendRequest), any());
    }

    @Test
    public void shouldDeleteMessage() {
        final String receiptHandle = "receiptHandle1";

        this.sqsQueue.deleteMessage(receiptHandle);

        verify(this.sqs)
            .deleteMessage(new DeleteMessageRequest().withQueueUrl("queueUrl1").withReceiptHandle(receiptHandle));
    }

    @Test
    public void shouldRetryMessage() {
        final String receiptHandle = "receiptHandle1";

        this.sqsQueue.retryMessage(receiptHandle);

        verify(this.sqs)
            .changeMessageVisibility(
                new ChangeMessageVisibilityRequest()
                    .withQueueUrl(queueUrl)
                    .withReceiptHandle(receiptHandle)
                    .withVisibilityTimeout(configuration.getRetrySeconds())
            );
    }

    @Test
    public void shouldReturnQueueArn() {
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest()
            .withQueueUrl(queueUrl)
            .withAttributeNames("QueueArn");
        when(this.sqs.getQueueAttributes(getQueueAttributesRequest))
            .thenReturn(new GetQueueAttributesResult().addAttributesEntry("QueueArn", "queueArn1"));

        final String queueArn = this.sqsQueue.getQueueArn();

        assertEquals("queueArn1", queueArn);

        verify(this.sqs).getQueueAttributes(getQueueAttributesRequest);
    }

    @Test
    public void shouldReceiveMessages() {
        List<Message> messages = Arrays.asList(
            new Message().withMessageId("messageId1"),
            new Message().withMessageId("messageId2")
        );

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
            .withWaitTimeSeconds(configuration.getWaitTimeSeconds())
            .withMaxNumberOfMessages(configuration.getMaxNumberOfMessages())
            .withVisibilityTimeout(configuration.getVisibilityTimeout());

        when(this.sqs.receiveMessage(receiveMessageRequest))
            .thenReturn(new ReceiveMessageResult().withMessages(messages));

        final List<Message> receivedMessages = this.sqsQueue.receiveMessages();

        assertEquals(messages, receivedMessages);

        verify(this.sqs).receiveMessage(receiveMessageRequest);
    }

    @Test
    public void shouldReceiveEmptyMessages() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
            .withWaitTimeSeconds(configuration.getWaitTimeSeconds())
            .withMaxNumberOfMessages(configuration.getMaxNumberOfMessages())
            .withVisibilityTimeout(configuration.getVisibilityTimeout());

        when(this.sqs.receiveMessage(receiveMessageRequest)).thenReturn(new ReceiveMessageResult());

        final List<Message> receivedMessages = this.sqsQueue.receiveMessages();

        assertEquals(Collections.emptyList(), receivedMessages);

        verify(this.sqs).receiveMessage(receiveMessageRequest);
    }

    @Test
    public void shouldEnableSNS() {
        final String topicArn = "topicArn1";

        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest()
            .withQueueUrl(queueUrl)
            .withAttributeNames("QueueArn");
        when(this.sqs.getQueueAttributes(getQueueAttributesRequest))
            .thenReturn(new GetQueueAttributesResult().addAttributesEntry("QueueArn", "queueArn1"));

        this.sqsQueue.enableSNS(topicArn);

        verify(this.sqs)
            .setQueueAttributes(
                new SetQueueAttributesRequest()
                    .withQueueUrl(this.queueUrl)
                    .addAttributesEntry(
                        "Policy",
                        new Policy()
                            .withStatements(
                                new Statement(Statement.Effect.Allow)
                                    .withPrincipals(Principal.AllUsers)
                                    .withActions(SQSActions.SendMessage)
                                    .withResources(new Resource("queueArn1"))
                                    .withConditions(
                                        new ArnCondition(
                                            ArnCondition.ArnComparisonType.ArnEquals,
                                            ConditionFactory.SOURCE_ARN_CONDITION_KEY,
                                            topicArn
                                        )
                                    )
                            )
                            .toJson()
                    )
            );

        verify(this.sqs).getQueueAttributes(getQueueAttributesRequest);
    }
}
