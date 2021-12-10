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
import com.amazonaws.services.sns.model.SubscribeRequest;
import de.tschumacher.queueservice.message.SQSMessage;
import de.tschumacher.queueservice.message.TestDO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SNSQueueStandardTest {
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

        when(this.sns.createTopic(new CreateTopicRequest().withName(configuration.getTopicName())))
            .thenReturn(new CreateTopicResult().withTopicArn(topicArn));

        this.snsQueue = new SNSQueue(this.sns, configuration);
    }

    @AfterEach
    public void shutDown() {
        verify(this.sns).createTopic(new CreateTopicRequest().withName("topicName1"));
        verifyNoMoreInteractions(this.sns);
    }

    @Test
    public void sendMessageTest() {
        SQSMessage<TestDO> sqsMessage = SQSMessage.<TestDO>builder().plainContent("content1").build();

        this.snsQueue.sendMessage(sqsMessage);

        verify(this.sns)
            .publishAsync(eq(new PublishRequest().withMessage("content1").withTopicArn(this.topicArn)), any());
    }

    @Test
    public void subscribeSQSQueueTest() {
        final String queueArn = "queueArn1";

        this.snsQueue.subscribeSQSQueue(queueArn);

        verify(this.sns)
            .subscribe(
                new SubscribeRequest()
                    .withTopicArn(topicArn)
                    .withEndpoint(queueArn)
                    .withProtocol("sqs")
                    .addAttributesEntry("RawMessageDelivery", "true")
            );
    }

    @Test
    public void getTopicArnTest() {
        final String resultTopicArn = this.snsQueue.getTopicArn();

        assertEquals(this.topicArn, resultTopicArn);
    }
}
