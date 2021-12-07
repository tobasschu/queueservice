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
package de.tschumacher.queueservice.message.coder;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.tschumacher.queueservice.message.TestDO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GsonSQSCoderTest {
    private GsonSQSCoder<TestDO> coder;
    private Gson gson;

    @BeforeEach
    public void setUp() {
        this.gson = new GsonBuilder().create();
        this.coder = new GsonSQSCoder<>(this.gson, TestDO.class);
    }

    @Test
    public void decodeTest() {
        final TestDO message = new TestDO("test1");

        final String decodeMessage = this.coder.decode(message);

        assertEquals(this.gson.toJson(message), decodeMessage);
    }

    @Test
    public void encodeTest() {
        final TestDO message = new TestDO("test1");
        final String decodedMessage = this.gson.toJson(message);

        final TestDO encodedMessage = this.coder.encode(decodedMessage);

        assertEquals(this.gson.fromJson(decodedMessage, TestDO.class).getContent(), encodedMessage.getContent());
    }
}
