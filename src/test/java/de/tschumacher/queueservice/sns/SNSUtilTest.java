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
package de.tschumacher.queueservice.sns;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.CreateTopicResult;
import de.tschumacher.queueservice.DataCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SNSUtilTest {
    private AmazonSNS sns;

    @BeforeEach
    public void setUp() {
        this.sns = Mockito.mock(AmazonSNS.class);
    }

    @AfterEach
    public void shutDown() {
        Mockito.verifyNoMoreInteractions(this.sns);
    }

    @Test
    public void createTest() {
        final CreateTopicResult createTopicResult = DataCreator.createCreateTopicResult();
        final String snsName = "snsName1";

        Mockito.when(this.sns.createTopic(snsName)).thenReturn(createTopicResult);

        final String createdTopicArn = SNSUtil.createTopic(this.sns, snsName);

        assertEquals(createTopicResult.getTopicArn(), createdTopicArn);

        Mockito.verify(this.sns).createTopic(snsName);
    }
}
