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
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.*;
import de.tschumacher.queueservice.message.SQSMessage;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQSQueue {
    private static final Logger logger = LoggerFactory.getLogger(SQSQueue.class);
    private final SQSQueueConfiguration configuration;
    private final AmazonSQSAsync sqs;
    private final String queueUrl;

    public SQSQueue(final SQSQueueConfiguration configuration) {
        this(configuration, createAmazonSQS(configuration));
    }

    public SQSQueue(final SQSQueueConfiguration configuration, final AmazonSQSAsync sqs) {
        this.configuration = configuration;
        this.sqs = sqs;
        queueUrl = getOrCreateQueue(sqs, configuration);
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

    private static String getOrCreateQueue(final AmazonSQS sqs, final SQSQueueConfiguration configuration) {
        String queueUrl;
        try {
            queueUrl = sqs.getQueueUrl(configuration.getQueueName()).getQueueUrl();
        } catch (final QueueDoesNotExistException e) {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest()
            .withQueueName(configuration.getQueueName());

            if (configuration.isFifo()) {
                createQueueRequest
                    .addAttributesEntry("FifoQueue", Boolean.toString(configuration.isFifo()))
                    .addAttributesEntry("ContentBasedDeduplication", "true");
            }
            queueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        }

        return queueUrl;
    }

    public synchronized List<Message> receiveMessages() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
            .withWaitTimeSeconds(configuration.getWaitTimeSeconds())
            .withMaxNumberOfMessages(configuration.getMaxNumberOfMessages())
            .withVisibilityTimeout(configuration.getVisibilityTimeout());

        return sqs.receiveMessage(receiveMessageRequest).getMessages();
    }

    public void deleteMessage(final String receiptHandle) {
        sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(queueUrl).withReceiptHandle(receiptHandle));
    }

    public void retryMessage(final String receiptHandle) {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest()
            .withQueueUrl(queueUrl)
            .withVisibilityTimeout(configuration.getRetrySeconds())
            .withReceiptHandle(receiptHandle);

        sqs.changeMessageVisibility(changeMessageVisibilityRequest);
    }

    public void sendMessage(final SQSMessage<?> sqsMessage) {
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
            .withQueueUrl(queueUrl)
            .withMessageBody(sqsMessage.getPlainContent())
            .withDelaySeconds(sqsMessage.getDelay());

        if (configuration.isFifo()) {
            sendMessageRequest.withMessageGroupId(sqsMessage.getMessageGroupId());
        }

        sqs.sendMessageAsync(
            sendMessageRequest,
            new AsyncHandler<SendMessageRequest, SendMessageResult>() {

                @Override
                public void onError(Exception e) {
                    logger.error("SQS send message failed.", e);
                }

                @Override
                public void onSuccess(SendMessageRequest request, SendMessageResult result) {
                    logger.debug("SQS message sent successfully: {}", result.getMessageId());
                }
            }
        );
    }

    public String getQueueArn() {
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest()
            .withQueueUrl(queueUrl)
            .withAttributeNames("QueueArn");

        return sqs.getQueueAttributes(getQueueAttributesRequest).getAttributes().get("QueueArn");
    }

    public void enableSNS(String topicArn) {
        SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest()
            .withQueueUrl(queueUrl)
            .addAttributesEntry("Policy", new Policy().withStatements(createPolicyStatement(topicArn)).toJson());

        sqs.setQueueAttributes(setQueueAttributesRequest);
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
