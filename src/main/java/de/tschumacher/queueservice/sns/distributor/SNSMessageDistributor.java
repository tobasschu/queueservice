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

import de.tschumacher.queueservice.message.SQSMessage;
import de.tschumacher.queueservice.message.SQSMessageFactory;
import de.tschumacher.queueservice.sns.SNSQueue;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SNSMessageDistributor<T> {
    private final SNSQueue snsQueue;
    private final SQSMessageFactory<T> factory;

    public void distribute(final T message) {
        this.distribute(message, null);
    }

    public void distribute(final T message, String messageGroupId) {
        SQSMessage<T> sqsMessage = this.factory.createSQSMessage(message);
        sqsMessage.setMessageGroupId(messageGroupId);

        this.snsQueue.sendMessage(sqsMessage);
    }
}
