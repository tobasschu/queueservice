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
import de.tschumacher.queueservice.message.SQSMessageFactory;
import de.tschumacher.queueservice.message.TestDO;
import de.tschumacher.queueservice.sqs.SQSQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SQSMessageDistributorTest {
    private SQSMessageFactory<TestDO> factory;
    private SQSMessageDistributor<TestDO> sqsMessageDistributor;
    private SQSQueue queue;

    @BeforeEach
    public void setUp() {
        this.queue = Mockito.mock(SQSQueue.class);
        this.factory = new SQSMessageFactory<>(new GsonSQSCoder<>(TestDO.class));
        this.sqsMessageDistributor = new SQSMessageDistributorImpl<>(this.queue, this.factory);
    }

    @AfterEach
    public void shutDown() {
        Mockito.verifyNoMoreInteractions(this.queue);
    }

    @Test
    public void distributeTest() {
        final TestDO message = new TestDO("testDO1");

        this.sqsMessageDistributor.distribute(message);

        Mockito.verify(this.queue).sendMessage(this.factory.createMessage(message).getPlainContent());
    }

    @Test
    public void distributeDelayedTest() {
        final TestDO message = new TestDO("testDO1");
        final int delay = 5;

        this.sqsMessageDistributor.distribute(message, delay);

        Mockito.verify(this.queue).sendMessage(this.factory.createMessage(message).getPlainContent(), delay);
    }
}
