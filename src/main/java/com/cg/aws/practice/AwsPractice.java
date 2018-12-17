package com.cg.aws.practice;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.File;
import java.util.Scanner;

public class AwsPractice {

    private static AWSCredentials awsCredentials;
    private static AmazonS3Client amazonS3Client;
    private static AmazonDynamoDBClient amazonDynamoDBClient;
    private static Scanner scanner;

    static {
        awsCredentials = new BasicAWSCredentials("AKIAIQEEQNUGLGJRUAKQ", "wPbigsbQs12wjXz0Og/NmHtEGSyCVW+GgwYSRBH2");
        amazonS3Client = new AmazonS3Client(awsCredentials);
        amazonDynamoDBClient = new AmazonDynamoDBClient(awsCredentials);
        scanner = new Scanner(System.in);
    }

    public void doJob() {

        //doS3RelatedJob();
        doDynamoDbRelatedJob();
    }

    private void doS3RelatedJob() {
        String opperationType = takeInputFromUser("Enter Bucket Opperation Type Either create or delete : ");
        if (opperationType.equalsIgnoreCase("create")) {
            String bucketName = createS3Bucket();
            insertIntoS3Bucket(bucketName, "D:/test.txt");
        } else if (opperationType.equalsIgnoreCase("delete")) {
            deleteS3Bucket();
        } else {
            System.out.println("Wrong Opperation Type!");
        }
    }

    private void doDynamoDbRelatedJob() {
        String opperationType = takeInputFromUser("Enter Table Opperation Type Either create or delete : ");
        if (opperationType.equalsIgnoreCase("create")) {
            createDynamoDbTable();
        } else if (opperationType.equalsIgnoreCase("delete")) {
            deleteDynamoDbTable();
        } else {
            System.out.println("Wrong Opperation Type!");
        }
    }

    private void createDynamoDbTable() {

        String tableName = takeInputFromUser("Enter Table Name : ");
        String check = "";
        while (isTableExist(tableName)) {
            check = takeInputFromUser("Table Already Exist Enter Another Name Or Press N To Exist : ");
            if (check.equalsIgnoreCase("N"))
                break;
            else
                tableName = check;
        }
        if (!check.equalsIgnoreCase("N")) {
            CreateTableRequest request = new CreateTableRequest().withTableName(tableName);

            String id = takeInputFromUser("Enter Partition Key : ");
            request.withKeySchema(new KeySchemaElement()
                    .withAttributeName(id)
                    .withKeyType(KeyType.HASH));

            request.withAttributeDefinitions(new AttributeDefinition()
                    .withAttributeName(id)
                    .withAttributeType(ScalarAttributeType.N));
            String atributeName = "", attributeType = "";
            takeTableColumsFromUser(request, atributeName);

            request.setProvisionedThroughput(new ProvisionedThroughput()
                    .withReadCapacityUnits(5l)
                    .withWriteCapacityUnits(2l));

            amazonDynamoDBClient.createTable(request);
            System.out.println("Table Created SuccessFully!");
        }

    }

    private void takeTableColumsFromUser(CreateTableRequest request, String atributeName) {
        String attributeType;
        while (!atributeName.equalsIgnoreCase("N")) {
            atributeName = takeInputFromUser("Enter Attribute Name Or Press N To Exist");
            if (!atributeName.equalsIgnoreCase("N")) {
                attributeType = takeInputFromUser("Enter Attribute Type As N(Number), S(String) and B(Byte)");
                if (attributeType.equalsIgnoreCase("N"))
                    request.withAttributeDefinitions(new AttributeDefinition()
                            .withAttributeName(atributeName)
                            .withAttributeType(ScalarAttributeType.N));
                else if (attributeType.equalsIgnoreCase("S"))
                    request.withAttributeDefinitions(new AttributeDefinition()
                            .withAttributeName(atributeName)
                            .withAttributeType(ScalarAttributeType.S));
                else if (attributeType.equalsIgnoreCase("B"))
                    request.withAttributeDefinitions(new AttributeDefinition()
                            .withAttributeName(atributeName)
                            .withAttributeType(ScalarAttributeType.B));
                else
                    System.out.println("Wrong Attribute Type!");
            }
        }
    }

    private void deleteDynamoDbTable() {
        String tableName = takeInputFromUser("Enter Table Name : ");
        if (isTableExist(tableName)) {
            amazonDynamoDBClient.deleteTable(tableName);
            System.out.println("Table Deleted SuccessFully!");
        } else
            System.out.println("Table Does Not Exist!");
    }

    private String createS3Bucket() {
        String bucketName = takeInputFromUser("Enter Bucket Name : ");
        String choice = "";
        while (amazonS3Client.doesBucketExist(bucketName)) {
            choice = takeInputFromUser("Duplicate bucket name please Enter another Bucket Name Or Press N To Continue With Same Bucket : ");
            if (choice.equalsIgnoreCase("N"))
                break;
            else
                bucketName = choice;
        }
        if (!choice.equalsIgnoreCase("N")) {
            amazonS3Client.createBucket(bucketName);
            System.out.println("Bucket Created Successfully!");
        }

        return bucketName;
    }

    private void deleteS3Bucket() {
        String bucketName = takeInputFromUser("Enter Bucket Name : ");
        if (amazonS3Client.doesBucketExist(bucketName)) {
            ObjectListing objectListing = amazonS3Client.listObjects(bucketName);
            for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
                amazonS3Client.deleteObject(bucketName, os.getKey());
                System.out.println("Object : " + os.getKey() + " get deleted successfully from : " + bucketName);
            }
            amazonS3Client.deleteBucket(bucketName);
            System.out.println("Bucket Deleted Successfully!");
        } else {
            System.out.println("Bucket Doesn't Exist!");
        }
    }

    private void insertIntoS3Bucket(String bucketName, String fileName) {
        String key = fileName.substring(fileName.lastIndexOf("/") + 1);
        amazonS3Client.putObject(bucketName, key, new File(fileName));
    }

    private String takeInputFromUser(String category) {
        System.out.println(category);
        return scanner.next();
    }

    private boolean isTableExist(String tableName) {
        try {
            amazonDynamoDBClient.describeTable(tableName);
            return true;
        } catch (Exception exception) {
            return false;
        }
    }

}
