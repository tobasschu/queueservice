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
package de.tschumacher.queueservice.message;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.sqs.model.Message;
import de.tschumacher.queueservice.message.coder.SQSCoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SQSMessageFactoryTest {
    private SQSMessageFactory<TestDO> factory;
    private SQSCoder<TestDO> coder;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() {
        this.coder = Mockito.mock(SQSCoder.class);
        this.factory = new SQSMessageFactory<>(coder);
    }

    @AfterEach
    public void shutDown() {
        Mockito.verifyNoMoreInteractions(this.coder);
    }

    @Test
    public void decodeMessageTest() {
        final String messageGroupId = "messageGroupId1";
        String plainContent = "plainContent1";
        final TestDO message = new TestDO("test1");

        Mockito.when(this.coder.decode(message)).thenReturn(plainContent);

        SQSMessage<TestDO> factoryMessage = factory.createSQSMessage(message, messageGroupId);

        assertEquals(
            factoryMessage,
            SQSMessage.builder().content(message).plainContent(plainContent).messageGroupId(messageGroupId).build()
        );

        Mockito.verify(this.coder).decode(message);
    }

    @Test
    public void decodeTest() {
        final TestDO testMessage = new TestDO("test1");
        final Message message = new Message()
            .withMessageId("messageId1")
            .withBody("body1")
            .withReceiptHandle("handle1")
            .addAttributesEntry("MessageGroupId", "messageGroupId1");

        Mockito.when(this.coder.encode("body1")).thenReturn(testMessage);

        SQSMessage<TestDO> factoryMessage = factory.createSQSMessage(message);

        assertEquals(
            factoryMessage,
            SQSMessage
                .builder()
                .content(testMessage)
                .plainContent(message.getBody())
                .messageId("messageId1")
                .receiptHandle("handle1")
                .messageGroupId("messageGroupId1")
                .build()
        );

        Mockito.verify(this.coder).encode(message.getBody());
    }
}
