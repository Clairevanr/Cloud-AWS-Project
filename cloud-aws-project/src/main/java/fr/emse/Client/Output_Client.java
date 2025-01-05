package fr.emse.Client;


import java.io.IOException;
import java.nio.file.Files;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static fr.emse.Characteristics.*;


public class Output_Client {

    public static void main(String[] args) throws IOException {

    
        S3Client s3 = S3Client.builder().region(REGION).build();
        
        

         //Check if the bucket exists:
         if (Upload_Client.DoesExist(s3,FINAL_BUCKET)){
            System.out.println("Bucket '" + FINAL_BUCKET + "' already exists.");
        }
        // if no, create the bucket
        else{
            CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(FINAL_BUCKET)
                .build();
            s3.createBucket(createBucketRequest);
            System.out.println("Bucket '" + FINAL_BUCKET + "' has been created.");
        }

        s3.close();
        Files.delete(OUTPUT_FILE_PATH);
        

    }

    
    
}
