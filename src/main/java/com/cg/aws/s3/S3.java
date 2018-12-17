package com.cg.aws.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.cg.aws.util.Util;

import java.io.File;

public class S3 {

    private static AmazonS3Client amazonS3Client;

    static {
        amazonS3Client = new AmazonS3Client(Util.getAwsCredentials());
    }

    public void doS3RelatedJob() {
        String opperationType = Util.takeInputFromUser("Enter Bucket Opperation Type Either create or delete : ");
        if (opperationType.equalsIgnoreCase("create")) {
            String bucketName = createS3Bucket();
            insertIntoS3Bucket(bucketName, "D:/test.txt");
        } else if (opperationType.equalsIgnoreCase("delete")) {
            deleteS3Bucket();
        } else {
            System.out.println("Wrong Opperation Type!");
        }
    }

    private String createS3Bucket() {
        String bucketName = Util.takeInputFromUser("Enter Bucket Name : ");
        String choice = "";
        while (amazonS3Client.doesBucketExist(bucketName)) {
            choice = Util.takeInputFromUser("Duplicate bucket name please Enter another Bucket Name Or Press N To Continue With Same Bucket : ");
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
        String bucketName = Util.takeInputFromUser("Enter Bucket Name : ");
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

}
