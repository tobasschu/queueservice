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

import de.tschumacher.queueservice.message.coder.SQSCoder;

public class SQSMessageFactory<F> {
    private final SQSCoder<F> coder;

    public SQSMessageFactory(final SQSCoder<F> coder) {
        super();
        this.coder = coder;
    }

    public SQSMessage<F> createMessage(final String body) {
        return new SQSMessage<F>(this.coder, body);
    }

    public SQSMessage<F> createMessage(final F body) {
        return new SQSMessage<F>(this.coder, body);
    }
}
