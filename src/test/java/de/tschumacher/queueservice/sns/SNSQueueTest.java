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
package de.tschumacher.queueservice.sns;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import de.tschumacher.queueservice.message.SQSMessage;
import de.tschumacher.queueservice.message.TestDO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SNSQueueTest {
    private SNSQueue snsQueue;

    @Mock
    private AmazonSNSAsync sns;

    private final String topicArn = "topicArn1";

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        SNSQueueConfiguration configuration = SNSQueueConfiguration
            .builder()
            .topicName("topicName1")
            .secretKey("secretKey1")
            .accessKey("accessKey1")
            .build();

        when(
                this.sns.createTopic(
                        new CreateTopicRequest()
                            .withName(configuration.getTopicName())
                            .addAttributesEntry("FifoTopic", "false")
                    )
            )
            .thenReturn(new CreateTopicResult().withTopicArn(topicArn));

        this.snsQueue = new SNSQueue(this.sns, configuration);
    }

    @AfterEach
    public void shutDown() {
        verify(this.sns)
            .createTopic(new CreateTopicRequest().withName("topicName1").addAttributesEntry("FifoTopic", "false"));
        verifyNoMoreInteractions(this.sns);
    }

    @Test
    public void shouldCreateFifoSNSQueue() {
        SNSQueueConfiguration configuration = SNSQueueConfiguration
            .builder()
            .topicName("topicName1.fifo")
            .secretKey("secretKey1")
            .accessKey("accessKey1")
            .build();

        when(
                this.sns.createTopic(
                        new CreateTopicRequest()
                            .withName(configuration.getTopicName())
                            .addAttributesEntry("FifoTopic", "true")
                    )
            )
            .thenReturn(new CreateTopicResult().withTopicArn(topicArn));

        this.snsQueue = new SNSQueue(this.sns, configuration);

        verify(this.sns)
            .createTopic(new CreateTopicRequest().withName("topicName1.fifo").addAttributesEntry("FifoTopic", "true"));
    }

    @Test
    public void sendMessageTest() {
        SQSMessage<TestDO> sqsMessage = SQSMessage
            .<TestDO>builder()
            .plainContent("content1")
            .messageGroupId("messageGroup1")
            .build();

        this.snsQueue.sendMessage(sqsMessage);

        verify(this.sns)
            .publishAsync(
                new PublishRequest()
                    .withMessage("content1")
                    .withMessageGroupId("messageGroup1")
                    .withTopicArn(this.topicArn)
            );
    }

    @Test
    public void subscribeSQSQueueTest() {
        final String queueUrl = "queueUrl1";

        this.snsQueue.subscribeSQSQueue(queueUrl);

        verify(this.sns).subscribe(this.topicArn, "sqs", queueUrl);
    }

    @Test
    public void getTopicArnTest() {
        final String resultTopicArn = this.snsQueue.getTopicArn();

        assertEquals(this.topicArn, resultTopicArn);
    }
}
