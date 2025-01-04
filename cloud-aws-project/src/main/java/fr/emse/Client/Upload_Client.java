package fr.emse.Client;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;


public class Upload_Client {

    public static void main(String[] args) {

        
        String queueURL = "https://sqs.us-east-1.amazonaws.com/330112968061/messaging-app-queue";
        String bucket_name = "myprojectbucket355552555";
        String path = "/home/clairevanruymbeke/Cloud-AWS-Final-project/data";
        String fileName = "data-20221106.csv";



        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();

        SqsClient sqs = SqsClient.builder().region(region).build();


        //Check if the bucket exists:
        if (DoesExist(s3,bucket_name)){
            System.out.println("Bucket '" + bucket_name + "' already exists.");
        }
        // if no, create the bucket
        else{
            CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(bucket_name)
                .build();
            s3.createBucket(createBucketRequest);
            System.out.println("Bucket '" + bucket_name + "' has been created.");
        }

        uploadFileToS3(s3, bucket_name, path + File.separator + fileName);
        sendMessageToSqs(sqs, queueURL, bucket_name, fileName);

         // Close the S3 client after use
         s3.close();

    }
    // Function defined to check the existence of the bucket
    public static boolean DoesExist(S3Client s3, String bucketName){

        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder()
        .build();
        ListBucketsResponse listBucketResponse = s3.listBuckets(listBucketsRequest);

        for (Bucket bucket : listBucketResponse.buckets()){
            if (bucket.name().equals(bucketName)){
                return true;
            }

        }
        return false;
        
    }


    public static void uploadFileToS3(S3Client s3, String bucketName, String filePath) {
        try {
            // Create the PutObjectRequest
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(Paths.get(filePath).getFileName().toString()) // Use the file name as the key
                    .build();

            // Upload the file
            s3.putObject(putObjectRequest, Paths.get(filePath));
            System.out.println("File '" + filePath + "' uploaded to bucket '" + bucketName + "' as '" + Paths.get(filePath).getFileName() + "'");
        } catch (S3Exception e) {
            System.err.println("Error uploading file: " + e.getMessage());
        }
    }

     private static void sendMessageToSqs(SqsClient sqs, String queueURL, String bucketName, String fileName) {
        try {
            String messageBody = "File uploaded: " + fileName + " in bucket: " + bucketName;

            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueURL)
                    .messageBody(messageBody)
                    .build();

            sqs.sendMessage(sendMsgRequest);
            System.out.println("Message sent to SQS queue: " + queueURL);
        } catch (Exception e) {
            System.err.println("Error sending message to SQS: " + e.getMessage());
        }
    }

}

