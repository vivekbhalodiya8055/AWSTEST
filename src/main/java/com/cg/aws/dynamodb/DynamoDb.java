package com.cg.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.cg.aws.util.Util;

public class DynamoDb {
    private static AmazonDynamoDBClient amazonDynamoDBClient;

    static {
        amazonDynamoDBClient = new AmazonDynamoDBClient(Util.getAwsCredentials());
    }

    public void doDynamoDbRelatedJob() {
        String opperationType = Util.takeInputFromUser("Enter Table Opperation Type Either create or delete : ");
        if (opperationType.equalsIgnoreCase("create")) {
            createDynamoDbTable();
        } else if (opperationType.equalsIgnoreCase("delete")) {
            deleteDynamoDbTable();
        } else {
            System.out.println("Wrong Opperation Type!");
        }
    }

    private void createDynamoDbTable() {

        String tableName = Util.takeInputFromUser("Enter Table Name : ");
        String check = "";
        while (isTableExist(tableName)) {
            check = Util.takeInputFromUser("Table Already Exist Enter Another Name Or Press N To Exist : ");
            if (check.equalsIgnoreCase("N"))
                break;
            else
                tableName = check;
        }
        if (!check.equalsIgnoreCase("N")) {
            CreateTableRequest request = new CreateTableRequest().withTableName(tableName);

            String id = Util.takeInputFromUser("Enter Partition Key : ");
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
            atributeName = Util.takeInputFromUser("Enter Attribute Name Or Press N To Exist");
            if (!atributeName.equalsIgnoreCase("N")) {
                attributeType = Util.takeInputFromUser("Enter Attribute Type As N(Number), S(String) and B(Byte)");
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
        String tableName = Util.takeInputFromUser("Enter Table Name : ");
        if (isTableExist(tableName)) {
            amazonDynamoDBClient.deleteTable(tableName);
            System.out.println("Table Deleted SuccessFully!");
        } else
            System.out.println("Table Does Not Exist!");
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
