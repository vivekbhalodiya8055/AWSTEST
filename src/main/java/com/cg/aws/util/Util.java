package com.cg.aws.util;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import java.util.Scanner;

public class Util {

    private static AWSCredentials awsCredentials;
    private static Scanner scanner;

    static{
        awsCredentials = new BasicAWSCredentials("************", "*********************");
        scanner = new Scanner(System.in);
    }

    public static String takeInputFromUser(String category) {
        System.out.println(category);
        return scanner.next();
    }

    public static AWSCredentials getAwsCredentials(){
        return awsCredentials;
    }
}
