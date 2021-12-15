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
package de.tschumacher.queueservice.sqs.distributor;

import de.tschumacher.queueservice.message.coder.GsonSQSCoder;
import de.tschumacher.queueservice.message.SQSMessage;
import de.tschumacher.queueservice.message.SQSMessageFactory;
import de.tschumacher.queueservice.message.TestDO;
import de.tschumacher.queueservice.sqs.SQSQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class SQSMessageDistributorTest {
    private SQSMessageDistributor<TestDO> sqsMessageDistributor;

    @Mock
    private SQSQueue queue;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        this.sqsMessageDistributor =
            new SQSMessageDistributor<>(this.queue, new SQSMessageFactory<>(new GsonSQSCoder<>(TestDO.class)));
    }

    @AfterEach
    public void shutDown() {
        Mockito.verifyNoMoreInteractions(this.queue);
    }

    @Test
    public void shouldDistributeMessage() {
        final TestDO message = new TestDO("testDO1");

        this.sqsMessageDistributor.distribute(message);

        SQSMessage<TestDO> sqsMessage = SQSMessage
            .<TestDO>builder()
            .content(message)
            .plainContent("{\"content\":\"testDO1\"}")
            .build();

        Mockito.verify(this.queue).sendMessage(sqsMessage);
    }

    @Test
    public void shouldDistributeMessageWithDelay() {
        final TestDO message = new TestDO("testDO1");
        final int delay = 5;

        this.sqsMessageDistributor.distribute(message, delay);

        SQSMessage<TestDO> sqsMessage = SQSMessage
            .<TestDO>builder()
            .content(message)
            .plainContent("{\"content\":\"testDO1\"}")
            .delay(delay)
            .build();

        Mockito.verify(this.queue).sendMessage(sqsMessage);
    }

    @Test
    public void shouldDistributeMessageWithMessageGroupId() {
        final TestDO message = new TestDO("testDO1");
        final String messageGroupId = "messageGroupId1";

        this.sqsMessageDistributor.distribute(message, messageGroupId);

        SQSMessage<TestDO> sqsMessage = SQSMessage
            .<TestDO>builder()
            .content(message)
            .plainContent("{\"content\":\"testDO1\"}")
            .messageGroupId(messageGroupId)
            .build();

        Mockito.verify(this.queue).sendMessage(sqsMessage);
    }

    @Test
    public void shouldDistributeMessageWithMessageGroupIdAndDelay() {
        final TestDO message = new TestDO("testDO1");
        final String messageGroupId = "messageGroupId1";
        final int delay = 5;

        this.sqsMessageDistributor.distribute(message, messageGroupId, delay);

        SQSMessage<TestDO> sqsMessage = SQSMessage
            .<TestDO>builder()
            .content(message)
            .plainContent("{\"content\":\"testDO1\"}")
            .messageGroupId(messageGroupId)
            .delay(delay)
            .build();

        Mockito.verify(this.queue).sendMessage(sqsMessage);
    }
}
