package com.cg.aws.sns;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.cg.aws.util.Util;

public class SQS {

    private static AmazonSQSClient amazonSQSClient;

    static {
        amazonSQSClient = new AmazonSQSClient(Util.getAwsCredentials());
    }

    public void doSQSRealatedJob() {
        String opperationType = Util.takeInputFromUser("Enter SQS Opperation Type Either create or delete : ");
        if (opperationType.equalsIgnoreCase("create")) {
            String queueName = createSQS();
            putMessageIntoQueue(queueName);
        } else if (opperationType.equalsIgnoreCase("delete")) {
            deleteQueue(Util.takeInputFromUser("Enter Queue Name : "));
        } else {
            System.out.println("Wrong Opperation Type!");
        }
    }

    private String createSQS() {
        String queueName = Util.takeInputFromUser("Enter Queue Name");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName)
                .addAttributesEntry("DelaySeconds", "60")
                .addAttributesEntry("MessageRetentionPeriod", "86400");
        String choice = "";
        while (!choice.equalsIgnoreCase("N")) {
            try {
                amazonSQSClient.createQueue(createQueueRequest);
                System.out.println("Queue Created Successfully!");
            } catch (Exception e) {
                System.out.println("Queue Already Exist");
                choice = Util.takeInputFromUser("Queue Already Exist, Enter Another Queue Name Or Press N");
                if (choice.equalsIgnoreCase("N"))
                    break;
                else
                    queueName = choice;
            }
        }
        return queueName;
    }

    private String getQueueUrl(String queueName){
        return amazonSQSClient.getQueueUrl(queueName).getQueueUrl();

    }

    private void putMessageIntoQueue(String queueName){
        SendMessageRequest sendMessageRequest=new SendMessageRequest()
                .withQueueUrl(getQueueUrl(queueName))
                .withMessageBody(Util.takeInputFromUser("Enter Message : "));
        amazonSQSClient.sendMessage(sendMessageRequest);
    }

    private void deleteQueue(String queueName){
        amazonSQSClient.deleteQueue(getQueueUrl(queueName));
    }
}
