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

import static de.tschumacher.queueservice.DataCreator.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.model.CreateTopicResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SNSQueueTest {
    private SNSQueue snsQueue;
    private AmazonSNSAsync sns;
    private String snsName;
    private String topicArn;

    @BeforeEach
    public void setUp() {
        this.snsName = "snsName1";
        this.sns = mock(AmazonSNSAsync.class);

        final CreateTopicResult createTopicResult = createCreateTopicResult();
        this.topicArn = createTopicResult.getTopicArn();

        when(this.sns.createTopic(this.snsName)).thenReturn(createTopicResult);

        this.snsQueue = new SNSQueue(this.sns, this.snsName);
    }

    @AfterEach
    public void shutDown() {
        verify(this.sns).createTopic(this.snsName);
        verifyNoMoreInteractions(this.sns);
    }

    @Test
    public void sendMessageTest() {
        final String message = "message1";

        this.snsQueue.sendMessage(message);

        verify(this.sns).publishAsync(this.topicArn, message);
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
