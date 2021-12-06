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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import de.tschumacher.queueservice.DataCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SQSUtilTest {
    private AmazonSQS sqs;

    @BeforeEach
    public void setUp() {
        this.sqs = Mockito.mock(AmazonSQS.class);
    }

    @AfterEach
    public void shutDown() {
        Mockito.verifyNoMoreInteractions(this.sqs);
    }

    @Test
    public void createTest() {
        final CreateQueueResult createQueueResult = DataCreator.createCreateQueueResult();
        final String queueName = "queueName1";

        Mockito.when(this.sqs.createQueue(queueName)).thenReturn(createQueueResult);

        final String createdQueueUrl = SQSUtil.create(this.sqs, queueName);

        assertEquals(createQueueResult.getQueueUrl(), createdQueueUrl);

        Mockito.verify(this.sqs).createQueue(queueName);
    }

    @Test
    public void getQueueUrlTest() {
        final GetQueueUrlResult getQueueUrlResult = DataCreator.createGetQueueUrlResult();
        final String queueName = "queueName1";

        Mockito.when(this.sqs.getQueueUrl(queueName)).thenReturn(getQueueUrlResult);

        final String createdQueueUrl = SQSUtil.getQueueUrl(this.sqs, queueName);

        assertEquals(getQueueUrlResult.getQueueUrl(), createdQueueUrl);

        Mockito.verify(this.sqs).getQueueUrl(queueName);
    }

    @Test
    public void createIfNotExistsExistsTest() {
        final GetQueueUrlResult getQueueUrlResult = DataCreator.createGetQueueUrlResult();
        final String queueName = "queueName1";

        Mockito.when(this.sqs.getQueueUrl(queueName)).thenReturn(getQueueUrlResult);

        final String createdQueueUrl = SQSUtil.createIfNotExists(this.sqs, queueName);

        assertEquals(getQueueUrlResult.getQueueUrl(), createdQueueUrl);

        Mockito.verify(this.sqs).getQueueUrl(queueName);
    }

    @Test
    public void createIfNotExistsNotExistsTest() {
        final CreateQueueResult createQueueResult = DataCreator.createCreateQueueResult();
        final String queueName = "queueName1";

        Mockito.when(this.sqs.getQueueUrl(queueName)).thenThrow(new QueueDoesNotExistException("queueName1"));
        Mockito.when(this.sqs.createQueue(queueName)).thenReturn(createQueueResult);

        final String createdQueueUrl = SQSUtil.createIfNotExists(this.sqs, queueName);

        assertEquals(createQueueResult.getQueueUrl(), createdQueueUrl);

        Mockito.verify(this.sqs).getQueueUrl(queueName);
        Mockito.verify(this.sqs).createQueue(queueName);
    }
}
