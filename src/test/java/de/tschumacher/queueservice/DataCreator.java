package de.tschumacher.queueservice;

import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class DataCreator {

    public static CreateQueueResult createCreateQueueResult() {
        final CreateQueueResult createQueueResult = new CreateQueueResult();
        createQueueResult.setQueueUrl("queueUrl");
        return createQueueResult;
    }

    public static GetQueueUrlResult createGetQueueUrlResult() {
        return new GetQueueUrlResult().withQueueUrl("queueUrl");
    }

    public static ReceiveMessageResult createReceiveMessageResult() {
        return new ReceiveMessageResult().withMessages(createMessage());
    }

    public static Message createMessage() {
        return new Message().withReceiptHandle("handle1").withBody("body1");
    }

    public static CreateTopicResult createCreateTopicResult() {
        return new CreateTopicResult().withTopicArn("topicArn1");
    }

    public static GetQueueAttributesResult createGetQueueAttributesResult() {
        return new GetQueueAttributesResult().addAttributesEntry("QueueArn", "queueArn1");
    }

    public static String createSnsNotificationJsonString() {
        return (
            "{" +
            "  \"Type\" : \"Notification\"," +
            "  \"MessageId\" : \"c5b7150e-2a56-52a3-92db-46f0dec8ca88\"," +
            "  \"TopicArn\" : \"arn:aws:sns:eu-central-1:850399106808:CENQStaging\"," +
            "  \"Message\" : \"{\\\"eventType\\\":\\\"ASSIGNMENT_CONFIRMED\\\",\\\"rawMessage\\\":\\\"{\\\\\\\"assignment\\\\\\\":{\\\\\\\"customerId\\\\\\\":\\\\\\\"sdas\\\\\\\"}}\\\"}\"," +
            "  \"Timestamp\" : \"2018-08-28T06:57:34.882Z\"," +
            "  \"SignatureVersion\" : \"1\"," +
            "  \"Signature\" : \"qTQ3EHPtLMwvffsjrZQI+w5Ap+KHnIwIBMnlGGWPIQukhIdwBsh/o4UBaQ3/T2bSsDvm6pwYttviL4HqRRD/me0KVHEqABbUvZq02I0Rz/dpwtz/3rHq/HDlc0iF3PiWalsRIyYxOYPxR0T35HN6FgFfM0j+fOxV7L3ubwVgnTO7QljenfGqfaxo+EuyR9P/x1Nm/Z/ud1i1k8wRFcqqvbRF4bMqc65EpkJQGvbV+2I1k0oostE5KlHvEVxC5J/92KwkHYV8jGWp+MwXRdq2gOh0kNC+o+QEoM/RkqltjQ9ogm/WbkrGHqd3GAxbLiv9qkcGLoKLxY7V5d0kkjoK1A==\"," +
            "  \"SigningCertURL\" : \"https://sns.eu-central-1.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem\"," +
            "  \"UnsubscribeURL\" : \"https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:850399106808:CENQStaging:8d0b8d04-b337-4257-a119-b141b4514c3c\"" +
            "}"
        );
    }
}
