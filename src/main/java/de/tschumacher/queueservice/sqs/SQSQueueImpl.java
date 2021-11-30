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
package de.tschumacher.queueservice.sqs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.auth.policy.conditions.ArnCondition;
import com.amazonaws.auth.policy.conditions.ArnCondition.ArnComparisonType;
import com.amazonaws.auth.policy.conditions.ConditionFactory;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SQSQueueImpl implements SQSQueue {
    private static final int VISIBILITY_TIMEOUT = 60 * 2;
    private static final int WAIT_TIME_SECONDS = 20;
    private static final int MAX_NUMBER_OF_MESSAGES = 1;
    private static final String DEFAULT_REGION = Regions.EU_CENTRAL_1.getName();

    private final AmazonSQSAsync sqs;
    private final String queueUrl;

    public SQSQueueImpl(AmazonSQSAsync sqs, final String queueName) {
        super();
        this.sqs = sqs;
        this.queueUrl = SQSUtil.createIfNotExists(this.sqs, queueName);
    }

    public SQSQueueImpl(final String accessKey, final String secretKey, final String queueName) {
        this(accessKey, secretKey, DEFAULT_REGION, queueName);
    }

    public SQSQueueImpl(final String accessKey, final String secretKey, String regionName, final String queueName) {
        this(createAmazonSQS(accessKey, secretKey, regionName), queueName);
    }

    private static AmazonSQSAsync createAmazonSQS(final String accessKey, final String secretKey, String regionName) {
        final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        return AmazonSQSAsyncClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.fromName(regionName))
            .build();
    }

    @Override
    public synchronized Message receiveMessage() {
        final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(this.queueUrl)
            .withWaitTimeSeconds(WAIT_TIME_SECONDS)
            .withMaxNumberOfMessages(MAX_NUMBER_OF_MESSAGES)
            .withVisibilityTimeout(VISIBILITY_TIMEOUT);
        final ReceiveMessageResult receiveMessage = this.sqs.receiveMessage(receiveMessageRequest);
        if (receiveMessage.getMessages().size() > 0) return receiveMessage.getMessages().get(0);
        return null;
    }

    @Override
    public void deleteMessage(final String receiptHandle) {
        this.sqs.deleteMessage(this.queueUrl, receiptHandle);
    }

    @Override
    public void changeMessageVisibility(final String receiptHandle, final int retrySeconds) {
        this.sqs.changeMessageVisibility(this.queueUrl, receiptHandle, retrySeconds);
    }

    @Override
    public void sendMessage(final String messageBody) {
        sendMessage(messageBody, null);
    }

    @Override
    public void sendMessage(final String messageBody, Integer delaySeconds) {
        final SendMessageRequest sendMessageRequest = new SendMessageRequest(this.queueUrl, messageBody);
        sendMessageRequest.setDelaySeconds(delaySeconds);
        this.sqs.sendMessageAsync(sendMessageRequest);
    }

    @Override
    public String getQueueArn() {
        final GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(
            this.queueUrl,
            Arrays.asList("QueueArn")
        );
        return this.sqs.getQueueAttributes(getQueueAttributesRequest).getAttributes().get("QueueArn");
    }

    @Override
    public void addSNSPermissions(String topicArn) {
        final Policy policy = createPolicy(topicArn);
        final Map<String, String> attributes = createAttributes(policy);

        final SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest(
            this.queueUrl,
            attributes
        );
        this.sqs.setQueueAttributes(setQueueAttributesRequest);
    }

    private Map<String, String> createAttributes(final Policy policy) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("Policy", policy.toJson());
        return attributes;
    }

    private Policy createPolicy(String topicArn) {
        final Policy policy = new Policy();
        policy.withStatements(createPolicyStatement(topicArn));
        return policy;
    }

    private Statement createPolicyStatement(String topicArn) {
        final Statement statement = new Statement(Statement.Effect.Allow);
        statement.withPrincipals(Principal.AllUsers);
        statement.withActions(SQSActions.SendMessage);
        statement.withResources(new Resource(getQueueArn()));
        statement.withConditions(
            new ArnCondition(ArnComparisonType.ArnEquals, ConditionFactory.SOURCE_ARN_CONDITION_KEY, topicArn)
        );
        return statement;
    }
}
