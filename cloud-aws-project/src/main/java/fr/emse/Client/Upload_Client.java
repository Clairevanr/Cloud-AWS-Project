package fr.emse.Client;

import java.io.File;
import java.nio.file.Paths;


import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import static fr.emse.Characteristics.*;


public class Upload_Client {

    public static void main(String[] args) {

        
    
    
       
        String fileName = "data-20221106.csv";


        S3Client s3 = S3Client.builder().region(REGION).build();

        SqsClient sqs = SqsClient.builder().region(REGION).build();


        //Check if the bucket exists:
        if (DoesExist(s3,BUCKET_NAME)){
            System.out.println("Bucket '" + BUCKET_NAME + "' already exists.");
        }
        // if no, create the bucket
        else{
            CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(BUCKET_NAME)
                .build();
            s3.createBucket(createBucketRequest);
            System.out.println("Bucket '" + BUCKET_NAME + "' has been created.");
        }

        uploadFileToS3(s3, BUCKET_NAME, DATA_REPOSITORY + File.separator + fileName);
        sendMessageToSqs(sqs, QUEUE_URL, BUCKET_NAME, fileName);

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

