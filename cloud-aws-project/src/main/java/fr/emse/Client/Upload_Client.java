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


        S3Client s3 = S3Client.builder().region(REGION).build();

        SqsClient sqs = SqsClient.builder().region(REGION).build();


        DoesExist(s3, SOURCE_BUCKET);
        uploadFileToS3(s3, SOURCE_BUCKET, DATA_REPOSITORY + File.separator + FILENAME);
        sendMessageToSqs(sqs, SQS_SUMMARIZE, SOURCE_BUCKET, FILENAME);

        
         s3.close();

    }
    // Function defined to check the existence of the bucket
    public static boolean DoesExist(S3Client s3, String bucketName){

        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder()
        .build();
        ListBucketsResponse listBucketResponse = s3.listBuckets(listBucketsRequest);

        for (Bucket bucket : listBucketResponse.buckets()){
            if (bucket.name().equals(bucketName)){
                System.out.println("Bucket '" + SOURCE_BUCKET + "' already exists.");
                return true;
            }

        }
        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(SOURCE_BUCKET)
                .build();
            s3.createBucket(createBucketRequest);
            System.out.println("Bucket '" + SOURCE_BUCKET + "' has been created.");
        return false;
        
    }



    // Function defined to upload a file in the bucket
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

     public static void sendMessageToSqs(SqsClient sqs, String queueURL, String bucketName, String fileName) {
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
     
    //Function defined to delete a file from a bucket
    public static void deleteFileFromS3(S3Client s3Client, String bucketName, String key) {
        try {
            
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            
            s3Client.deleteObject(deleteRequest);

            
            System.out.println("Fichier supprimé avec succès : " + key 
                               + " du bucket : " + bucketName);
        } catch (Exception e) {
            
            System.err.println("Erreur lors de la suppression du fichier : " + key);
            e.printStackTrace();
        }
    }

}

