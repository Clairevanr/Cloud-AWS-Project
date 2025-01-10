package fr.emse.WorkersLambda;

import java.io.*;
import java.nio.charset.StandardCharsets;

import java.util.*;
import java.util.stream.Collectors;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;


import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;


import static fr.emse.Characteristics.*;



public class SummarizeLambda implements RequestHandler<SQSEvent, Object>{

    @Override
    public SQSBatchResponse handleRequest(SQSEvent sqsEvent, Context context) {
	 	for(SQSMessage msg : sqsEvent.getRecords()){
            String fileName = msg.getBody().substring(msg.getBody().indexOf(":") + 2, msg.getBody().indexOf(" in bucket:")).trim();
            processMessage(fileName, msg);
        }
        return null;
    }

    public static void processMessage(String fileName, SQSMessage msg){
        SqsClient sqsClient = SqsClient.builder().region(REGION).build();
        S3Client s3Client = S3Client.builder().region(REGION).build();
        
        ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(SOURCE_BUCKET).build();
                
        ListObjectsResponse res = s3Client.listObjects(listObjects);
        List<S3Object> objects = res.contents();	

        if (objects.stream().anyMatch((S3Object x) -> x.key().equals(fileName))) {
                        
            // To retrieve the file and process it

            GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(SOURCE_BUCKET)
                .key(FILENAME)
                .build();

            // Retrieve the file from s3 bucket
            
            ResponseInputStream<GetObjectResponse> s3ObjectStream = s3Client.getObject(objectRequest);
            
            uploadData(s3Client, s3ObjectStream,FINAL_BUCKET, objects);

            deleteFileFromS3(s3Client, SOURCE_BUCKET, fileName);
            sendMessageToSqs(sqsClient, SQS_CONSOLIDATOR,FINAL_BUCKET ,"summarized-data.csv");

            s3Client.close();
            
            System.out.println("Operation Successful");
            
        } else {
            System.out.println("The file is not in the bucket");
        }

        sqsClient.close();
    }



    public static void uploadData(S3Client s3, ResponseInputStream<GetObjectResponse> data, String bucketName, List<S3Object> objects) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(data, StandardCharsets.UTF_8))) {
            Map<String, Summary> summaryMap = new HashMap<>();
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");

            // Read CSV header
            String header = reader.readLine();
            if (header == null) {
                throw new IllegalArgumentException("The CSV file is empty.");
            }

            // Process each line of the CSV
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(","); // Assuming tab-separated values
                if (columns.length < 9) continue; // Skip invalid rows

                String srcIp = columns[1];
                String dstIp = columns[3];
                String timestamp = columns[6];
                long flowDuration = Long.parseLong(columns[7]);
                long totFwdPkts = Long.parseLong(columns[8]);

                // Parse date from timestamp
                LocalDate date = LocalDate.parse(timestamp.split(" ")[0], dateFormatter);

                // Create a unique key for (srcIp, dstIp, date)
                String key = srcIp + "-" + dstIp + "-" + date;

                // Aggregate data
                summaryMap.computeIfAbsent(key, k -> new Summary(srcIp, dstIp, date))
                          .add(flowDuration, totFwdPkts);
            }

            // Prepare summarized output
            String output = "Src IP\tDst IP\tDate\tTotal Flow Duration\tTotal Forward Packets\n";
            output += summaryMap.values().stream()
                                .map(Summary::toString)
                                .collect(Collectors.joining("\n"));

            // Upload summarized data to S3
            String outputKey = "summarized-data.csv";
            s3.putObject(builder -> builder.bucket(bucketName).key(outputKey).build(),
                    RequestBody.fromString(output));

            System.out.println("Summary uploaded successfully to S3: " + outputKey);

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error processing the CSV file: " + e.getMessage(), e);
        }
    }

    // Helper class for summarizing data
    private static class Summary {
        private final String srcIp;
        private final String dstIp;
        private final LocalDate date;
        private long totalFlowDuration;
        private long totalForwardPackets;

        public Summary(String srcIp, String dstIp, LocalDate date) {
            this.srcIp = srcIp;
            this.dstIp = dstIp;
            this.date = date;
            this.totalFlowDuration = 0;
            this.totalForwardPackets = 0;
        }

        public void add(long flowDuration, long totFwdPkts) {
            this.totalFlowDuration += flowDuration;
            this.totalForwardPackets += totFwdPkts;
        }

        @Override
        public String toString() {
            return srcIp + "\t" + dstIp + "\t" + date + "\t" + totalFlowDuration + "\t" + totalForwardPackets;
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