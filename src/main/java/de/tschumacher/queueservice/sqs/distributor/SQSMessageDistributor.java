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

import de.tschumacher.queueservice.message.SQSMessage;
import de.tschumacher.queueservice.message.SQSMessageFactory;
import de.tschumacher.queueservice.sqs.SQSQueue;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SQSMessageDistributor<T> {
    private final SQSQueue sqsQueue;
    private final SQSMessageFactory<T> factory;

    public void distribute(final T message) {
        distribute(message, null, null);
    }

    public void distribute(final T message, Integer delay) {
        distribute(message, null, delay);
    }

    public void distribute(final T message, String messageGroupId) {
        distribute(message, messageGroupId, null);
    }

    public void distribute(final T message, String messageGroupId, Integer delay) {
        SQSMessage<T> sqsMessage = this.factory.createSQSMessage(message);
        sqsMessage.setMessageGroupId(messageGroupId);
        sqsMessage.setDelay(delay);

        this.sqsQueue.sendMessage(sqsMessage);
    }
}
