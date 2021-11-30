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
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;

public class SNSQueueImpl implements SNSQueue {
    private static final String DEFAULT_REGION = Regions.EU_CENTRAL_1.getName();
    private final AmazonSNSAsync sns;
    private final String topicArn;

    public SNSQueueImpl(AmazonSNSAsync sns, final String snsName) {
        super();
        this.sns = sns;
        this.topicArn = SNSUtil.createTopic(this.sns, snsName);
    }

    public SNSQueueImpl(final String accessKey, final String secretKey, final String snsName) {
        this(accessKey, secretKey, DEFAULT_REGION, snsName);
    }

    public SNSQueueImpl(final String accessKey, final String secretKey, String regionName, final String snsName) {
        this(createAmazonSQS(accessKey, secretKey, regionName), snsName);
    }

    private static AmazonSNSAsync createAmazonSQS(final String accessKey, final String secretKey, String regionName) {
        final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        return AmazonSNSAsyncClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.fromName(regionName))
            .build();
    }

    @Override
    public void sendMessage(String message) {
        this.sns.publishAsync(getTopicArn(), message);
    }

    @Override
    public void subscribeSQSQueue(String queueArn) {
        this.sns.subscribe(getTopicArn(), "sqs", queueArn);
    }

    @Override
    public String getTopicArn() {
        return this.topicArn;
    }
}
