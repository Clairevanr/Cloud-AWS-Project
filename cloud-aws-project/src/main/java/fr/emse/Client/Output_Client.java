package fr.emse.Client;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class Output_Client {

    public static void main(String[] args) {

        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder().region(region).build();
        String bucket_name = "myprojectbucket355552555";
        String outputPath =  "/home/clairevanruymbeke/Cloud-AWS-Final-project/data/traffic_stats/stats.csv";

        if (Upload_Client.DoesExist(s3, bucket_name)){
            Upload_Client.uploadFileToS3(s3, bucket_name, outputPath);
        }

        s3.close();
        

    }

    
    
}
