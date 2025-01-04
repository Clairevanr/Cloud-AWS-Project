package fr.emse.Client;


import java.io.IOException;
import java.nio.file.Files;

import software.amazon.awssdk.services.s3.S3Client;
import static fr.emse.Characteristics.*;

public class Output_Client {

    public static void main(String[] args) throws IOException {

    
        S3Client s3 = S3Client.builder().region(REGION).build();
        
        

        if (Upload_Client.DoesExist(s3, BUCKET_NAME)){
            Upload_Client.uploadFileToS3(s3, BUCKET_NAME, OUTPUT_FILE_PATH.toString());
        }

        s3.close();
        Files.delete(OUTPUT_FILE_PATH);
        

    }

    
    
}
