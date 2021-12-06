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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;
import com.amazonaws.services.sns.model.CreateTopicRequest;

public class SNSQueue {
    private final SNSQueueConfiguration configuration;
    private final AmazonSNSAsync sns;
    private final String topicArn;

    public SNSQueue(final SNSQueueConfiguration configuration) {
        this.configuration = configuration;
        this.sns = createAmazonSQS(configuration);
        this.topicArn = createTopic(this.sns, configuration);
    }

    public static String createTopic(AmazonSNS sns, SNSQueueConfiguration configuration) {
        CreateTopicRequest createTopicRequest = new CreateTopicRequest()
            .withName(configuration.getTopicName())
            .addAttributesEntry("FifoTopic", Boolean.toString(configuration.isFifo()));
        return sns.createTopic(createTopicRequest).getTopicArn();
    }

    private static AmazonSNSAsync createAmazonSQS(final SNSQueueConfiguration configuration) {
        final BasicAWSCredentials credentials = new BasicAWSCredentials(
            configuration.getAccessKey(),
            configuration.getSecretKey()
        );

        return AmazonSNSAsyncClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.fromName(configuration.getDefaultRegion()))
            .build();
    }

    public void sendMessage(String message) {
        //TODO use SQSMessage
        this.sns.publishAsync(this.topicArn, message);
    }

    public void subscribeSQSQueue(String queueArn) {
        this.sns.subscribe(getTopicArn(), "sqs", queueArn);
    }

    public String getTopicArn() {
        return this.topicArn;
    }
}
