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
package de.tschumacher.queueservice.sns.distributor;

import de.tschumacher.queueservice.message.coder.GsonSQSCoder;
import de.tschumacher.queueservice.message.SQSMessageFactory;
import de.tschumacher.queueservice.message.TestDO;
import de.tschumacher.queueservice.sns.SNSQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SNSMessageDistributorTest {
    private SNSMessageDistributor<TestDO> snsMessageDistributor;
    private SQSMessageFactory<TestDO> factory;
    private SNSQueue snsQueue;

    @BeforeEach
    public void setUp() {
        this.snsQueue = Mockito.mock(SNSQueue.class);
        this.factory = new SQSMessageFactory<>(new GsonSQSCoder<>(TestDO.class));
        this.snsMessageDistributor = new SNSMessageDistributorImpl<>(this.snsQueue, this.factory);
    }

    @AfterEach
    public void shutDown() {
        Mockito.verifyNoMoreInteractions(this.snsQueue);
    }

    @Test
    public void distributeTest() {
        final TestDO message = new TestDO("testDO1");

        this.snsMessageDistributor.distribute(message);

        Mockito.verify(this.snsQueue).sendMessage(this.factory.createMessage(message).getPlainContent());
    }
}
