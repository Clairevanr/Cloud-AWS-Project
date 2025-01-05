package fr.emse.SummarizeWorker;

import java.io.*;
import java.util.*;

import fr.emse.Client.Upload_Client;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;


import static fr.emse.Characteristics.*;



public class Summarize {

    public static void main(String[] args) {
    
        SqsClient sqsClient = SqsClient.builder().region(REGION).build();


        processMessage(SQS_SUMMARIZE,sqsClient);
        sqsClient.close();
    

    }

    // Function to check if the file exists in the bucket

    private static void processMessage(String queueURL, SqsClient sqsClient){

        
		

        // Reception of the queue's messages
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(queueURL).maxNumberOfMessages(10).build();
		List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

         

        if (!messages.isEmpty()) {
			S3Client s3 = S3Client.builder().region(REGION).build();


            ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(SOURCE_BUCKET).build();
			
			ListObjectsResponse res = s3.listObjects(listObjects);
			List<S3Object> objects = res.contents();	


            for(Message msg : messages) {
				String fileName = msg.body().substring(msg.body().indexOf(":") + 2, msg.body().indexOf(" in bucket:")).trim();
                
	
				if (objects.stream().anyMatch((S3Object x) -> x.key().equals(fileName))) {
                    
                    // To retrieve the file and process it
                    processFileFromS3(s3,sqsClient, SOURCE_BUCKET, fileName);

					
					
					// Delete the message from the queue
					DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueURL)
					 		.receiptHandle(msg.receiptHandle()).build();
	
					sqsClient.deleteMessage(deleteMessageRequest);

                    System.out.println("Operation Successful, now retrieving");
					
				} else {
					System.out.println("The file is not in the bucket");
				}
			}
            
		} else {
			System.out.println("The queue is empty");
		}
	}


    private static void processFileFromS3(S3Client s3Client, SqsClient sqsClient, String bucketName, String fileName) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .build();

            // Retrieve the file from s3 bucket
            ResponseInputStream<GetObjectResponse> s3ObjectStream = s3Client.getObject(objectRequest);
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3ObjectStream));
            reader.readLine(); // Skip the header
            String line;

            // Create the traffic Summary map
            Map<String, long[]> trafficSummaryMap = new HashMap<>();

            while ((line = reader.readLine()) != null) {
                processCsvLine(line, trafficSummaryMap);
            }


            
            writeSummaryToCsv(SUMMARY_FILE_PATH.toString(), trafficSummaryMap);
               
            //Check if the bucket exists:
            if (Upload_Client.DoesExist(s3Client,TEMP_BUCKET)){
                System.out.println("Bucket '" + TEMP_BUCKET + "' already exists.");
            }
            // if no, create the bucket
            else{
                CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                    .bucket(TEMP_BUCKET)
                    .build();
                s3Client.createBucket(createBucketRequest);
                System.out.println("Bucket '" + TEMP_BUCKET + "' has been created.");
            }

            Upload_Client.uploadFileToS3(s3Client, SOURCE_BUCKET, DATA_REPOSITORY + File.separator + FILENAME);
            Upload_Client.sendMessageToSqs(sqsClient, SQS_CONSOLIDATOR, SOURCE_BUCKET, FILENAME);

         // Close the S3 client after use
         s3Client.close();
         sqsClient.close();



        } catch (IOException e) {
            System.err.println("Error processing file '" + fileName + "': " + e.getMessage());
        }
    }


    private static void processCsvLine(String line, Map<String, long[]> trafficSummaryMap) {
        String[] fields = line.split(",");

        // Extract required fields
        String srcIP = fields[1];
        String dstIP = fields[3];
        String timestamp = fields[6];
        long flowDuration = Long.parseLong(fields[7]);
        long totFwdPkts = Long.parseLong(fields[8]);

        // Extract the date from the timestamp
        String date = timestamp.split(" ")[0];

        // Create a unique key for the combination
        String key = date + "-" + srcIP + "-" + dstIP;

        // Update the map
        trafficSummaryMap.computeIfAbsent(key, k -> new long[2]);
        trafficSummaryMap.get(key)[0] += flowDuration;  // Add Flow Duration
        trafficSummaryMap.get(key)[1] += totFwdPkts;    // Add Total Forward Packets
    }


    private static void writeSummaryToCsv(String outputFilePath, Map<String, long[]> trafficSummaryMap) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            writer.write("Date,Src IP,Dst IP,Total Flow Duration,Total Forward Packets\n");
            for (Map.Entry<String, long[]> entry : trafficSummaryMap.entrySet()) {
                String[] keyParts = entry.getKey().split("-");
                long[] metrics = entry.getValue();
                writer.write(String.join(",", keyParts[0], keyParts[1], keyParts[2],
                        String.valueOf(metrics[0]), String.valueOf(metrics[1])) + "\n");
            }
        

        } catch (IOException e) {
            System.err.println("Error writing summary CSV: " + e.getMessage());
        }
    }

    


    
    

    
}
