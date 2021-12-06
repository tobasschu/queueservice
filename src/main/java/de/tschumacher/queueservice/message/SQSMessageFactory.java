/*
   Copyright 2015 Tobias Schumacher

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package de.tschumacher.queueservice.message;

import com.amazonaws.services.sqs.model.Message;
import de.tschumacher.queueservice.message.coder.SQSCoder;

public class SQSMessageFactory<F> {
    private final SQSCoder<F> coder;

    public SQSMessageFactory(SQSCoder<F> coder) {
        super();
        this.coder = coder;
    }

    public SQSMessage<F> createMessage(Message message) {
        F content = coder.encode(message.getBody());
        return SQSMessage
            .<F>builder()
            .content(content)
            .plainContent(message.getBody())
            .messageId(message.getMessageId())
            .messageGroupId(message.getAttributes().get("MessageGroupId"))
            .receiptHandle(message.getReceiptHandle())
            .build();
    }

    public SQSMessage<F> createMessage(F body, String messageGroupId) {
        String plainContent = coder.decode(body);
        return SQSMessage.<F>builder().content(body).plainContent(plainContent).messageGroupId(messageGroupId).build();
    }

    public SQSMessage<F> createMessage(F body) {
        return createMessage(body, null);
    }
}
