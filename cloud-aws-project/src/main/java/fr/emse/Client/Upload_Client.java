package fr.emse.Client;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;


public class Upload_Client {

    public static void main(String[] args) {

        
        String queueURL = "";
        String bucket_name = "myprojectbucket355552555";
        String path = Path.of("/home/clairevanruymbeke/Cloud-AWS-Final-project/data").toString();
        String fileName = "data-20221106.csv";


        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();

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


    private static void uploadFileToS3(S3Client s3, String bucketName, String filePath) {
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

}

