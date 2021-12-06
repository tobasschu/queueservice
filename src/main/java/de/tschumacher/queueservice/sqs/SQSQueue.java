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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.*;
import de.tschumacher.queueservice.message.SQSMessage;
import java.util.*;

public class SQSQueue {
    private final SQSQueueConfiguration configuration;
    private final AmazonSQSAsync sqs;
    private final String queueUrl;

    public SQSQueue(final SQSQueueConfiguration configuration) {
        this.configuration = configuration;
        this.sqs = createAmazonSQS(configuration);
        this.queueUrl = getOrCreateQueue(this.sqs, configuration);
    }

    private static AmazonSQSAsync createAmazonSQS(final SQSQueueConfiguration configuration) {
        final BasicAWSCredentials credentials = new BasicAWSCredentials(
            configuration.getAccessKey(),
            configuration.getSecretKey()
        );

        return AmazonSQSAsyncClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.fromName(configuration.getDefaultRegion()))
            .build();
    }

    public static String getOrCreateQueue(final AmazonSQS sqs, final SQSQueueConfiguration configuration) {
        String queueUrl;
        try {
            queueUrl = sqs.getQueueUrl(configuration.getQueueName()).getQueueUrl();
        } catch (final QueueDoesNotExistException e) {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(configuration.getQueueName())
                .addAttributesEntry("FifoQueue", Boolean.toString(configuration.isFifo()));

            queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        }

        return queueUrl;
    }

    public synchronized List<Message> receiveMessages() {
        final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(this.queueUrl)
            .withWaitTimeSeconds(configuration.getWaitTimeSeconds())
            .withMaxNumberOfMessages(configuration.getMaxNumberOfMessages())
            .withVisibilityTimeout(configuration.getVisibilityTimeout());

        return this.sqs.receiveMessage(receiveMessageRequest).getMessages();
    }

    public void deleteMessage(final String receiptHandle) {
        this.sqs.deleteMessage(this.queueUrl, receiptHandle);
    }

    public void retryMessage(final String receiptHandle) {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest()
            .withQueueUrl(queueUrl)
            .withVisibilityTimeout(this.configuration.getRetrySeconds())
            .withReceiptHandle(receiptHandle);

        this.sqs.changeMessageVisibility(changeMessageVisibilityRequest);
    }

    public void sendMessage(final SQSMessage<?> sqsMessage) {
        final SendMessageRequest sendMessageRequest = new SendMessageRequest()
            .withQueueUrl(this.queueUrl)
            .withMessageBody(sqsMessage.getPlainContent())
            .withDelaySeconds(sqsMessage.getDelay())
            .withMessageGroupId(sqsMessage.getMessageGroupId());

        this.sqs.sendMessageAsync(sendMessageRequest);
    }

    public String getQueueArn() {
        final GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest()
            .withQueueUrl(this.queueUrl)
            .withAttributeNames("QueueArn");

        return this.sqs.getQueueAttributes(getQueueAttributesRequest).getAttributes().get("QueueArn");
    }

    public void enableSNS(String topicArn) {
        final SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest()
            .withQueueUrl(this.queueUrl)
            .addAttributesEntry("Policy", new Policy().withStatements(createPolicyStatement(topicArn)).toJson());

        this.sqs.setQueueAttributes(setQueueAttributesRequest);
    }

    private Statement createPolicyStatement(String topicArn) {
        return new Statement(Statement.Effect.Allow)
            .withPrincipals(Principal.AllUsers)
            .withActions(SQSActions.SendMessage)
            .withResources(new Resource(getQueueArn()))
            .withConditions(
                new ArnCondition(ArnComparisonType.ArnEquals, ConditionFactory.SOURCE_ARN_CONDITION_KEY, topicArn)
            );
    }
}
