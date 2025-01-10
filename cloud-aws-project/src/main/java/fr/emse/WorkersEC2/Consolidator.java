package fr.emse.WorkersEC2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fr.emse.Client.Upload_Client;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;



public class Consolidator {

    public static void main(String[] args) throws IOException {
        while (true){
            SqsClient sqsClient = SqsClient.builder().region(REGION).build();
            S3Client s3Client = S3Client.builder().region(REGION).build();

            // Reception of the queue's messages
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(SQS_CONSOLIDATOR).maxNumberOfMessages(1).build();
            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

            

            if (!messages.isEmpty()) {
                
                ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(TEMP_BUCKET).build();
                
                ListObjectsResponse res = s3Client.listObjects(listObjects);
                List<S3Object> objects = res.contents();	


                for(Message msg : messages) {
                    String fileName = msg.body().substring(msg.body().indexOf(":") + 2, msg.body().indexOf(" in bucket:")).trim();
                    System.out.println(fileName);
                    objects.forEach(obj -> System.out.println("Clé dans le bucket : " + obj.key()));
                    

                    
        
                    if (objects.stream().anyMatch((S3Object x) -> x.key().equals(fileName.trim()))) {
                        
                        // To retrieve the file and process it

                        GetObjectRequest objectRequest = GetObjectRequest.builder()
                        .bucket(TEMP_BUCKET)
                        .key(fileName)
                        .build();

                        // Retrieve the file from s3 bucket
                        ResponseInputStream<GetObjectResponse> s3ObjectStream = s3Client.getObject(objectRequest);

                        try (BufferedReader summarizedReader = new BufferedReader(new InputStreamReader(s3ObjectStream))) {
                            consolidateCSVData(summarizedReader);
                            Files.delete(SUMMARY_FILE_PATH);
                        } catch (IOException e) {
                            System.out.println("Error reading the file: " + e.getMessage());
                        }
                        

                        //Check if the bucket exists:
                        Upload_Client.DoesExist(s3Client,FINAL_BUCKET);
                        // Upload the file to the final bucket
                        Upload_Client.uploadFileToS3(s3Client, FINAL_BUCKET, "data/stats/stats.csv");
                        // Delete the file from the temp bucket
                        Upload_Client.deleteFileFromS3(s3Client, TEMP_BUCKET, fileName);
                        

                        
                        s3Client.close();

                        // Delete the message from the queue
                        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(SQS_CONSOLIDATOR)
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
            sqsClient.close();
        }    

        
    
    }

    private static void consolidateCSVData(BufferedReader summarizedReader) throws IOException {
        // Maps to store data by Src IP -> Dst IP -> List of Flow Durations and Forward Packets
        Map<String, Map<String, List<Long>>> trafficDataFlowDuration = new HashMap<>();
        Map<String, Map<String, List<Integer>>> trafficDataForwardPackets = new HashMap<>();

        // Skip the header line
        String line = summarizedReader.readLine(); // Read and ignore the header

        // Read the file line by line
        while ((line = summarizedReader.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length < 5) continue;

            try {
                String srcIP = fields[1];
                String dstIP = fields[2];
                long flowDuration = Long.parseLong(fields[3]); // Total Flow Duration
                int forwardPackets = Integer.parseInt(fields[4]); // Total Forward Packets

                // Store the data in the respective maps
                trafficDataFlowDuration
                    .computeIfAbsent(srcIP, k -> new HashMap<>())
                    .computeIfAbsent(dstIP, k -> new ArrayList<>())
                    .add(flowDuration);

                trafficDataForwardPackets
                    .computeIfAbsent(srcIP, k -> new HashMap<>())
                    .computeIfAbsent(dstIP, k -> new ArrayList<>())
                    .add(forwardPackets);

            } catch (NumberFormatException e) {
                // Handle invalid number format (skip the line or log the error)
                System.out.println("Skipping invalid line: " + line);
            }
        }

        // Calculate stats (average, std dev) for both flow duration and forward packets
        writeTrafficStatsToCSV(trafficDataFlowDuration, trafficDataForwardPackets);
    }

    // Method to calculate standard deviation
    private static double calculateStandardDeviation(List<? extends Number> data) {
        double mean = data.stream().mapToDouble(Number::doubleValue).average().orElse(0.0);
        double variance = data.stream().mapToDouble(i -> Math.pow(i.doubleValue() - mean, 2)).average().orElse(0.0);
        return Math.sqrt(variance);
    }

    // Method to write the statistics to a CSV file
    private static void writeTrafficStatsToCSV(Map<String, Map<String, List<Long>>> trafficDataFlowDuration,
                                               Map<String, Map<String, List<Integer>>> trafficDataForwardPackets) throws IOException {

        

        // Create the directory if it doesn't exist
        Files.createDirectories(OUTPUT_FILE_PATH.getParent());

        try (BufferedWriter writer = Files.newBufferedWriter(OUTPUT_FILE_PATH)) {
            // Write the header row
            writer.write("Src IP,Dst IP,Total Flow Duration,Total Forward Packets,Avg Flow Duration,Avg Forward Packets,Std Dev Flow Duration,Std Dev Forward Packets\n");

            // Loop through the traffic data to calculate and write stats for each Src IP -> Dst IP combination
            for (String srcIP : trafficDataFlowDuration.keySet()) {
                for (String dstIP : trafficDataFlowDuration.get(srcIP).keySet()) {
                    List<Long> flowDurations = trafficDataFlowDuration.get(srcIP).get(dstIP);
                    List<Integer> forwardPackets = trafficDataForwardPackets.get(srcIP).get(dstIP);

                    // Calculate the averages
                    double avgFlowDuration = flowDurations.stream().mapToLong(Long::longValue).average().orElse(0.0);
                    double avgForwardPackets = forwardPackets.stream().mapToInt(Integer::intValue).average().orElse(0.0);

                    // Calculate the standard deviations
                    double stdDevFlowDuration = calculateStandardDeviation(flowDurations);
                    double stdDevForwardPackets = calculateStandardDeviation(forwardPackets);

                    // Get the total values (use the sum of the lists as totals)
                    long totalFlowDuration = flowDurations.stream().mapToLong(Long::longValue).sum();
                    int totalForwardPackets = forwardPackets.stream().mapToInt(Integer::intValue).sum();

                    // Write the results to the CSV file
                    writer.write(String.format("%s,%s,%d,%d,%.2f,%.2f,%.2f,%.2f\n", 
                            srcIP, dstIP, totalFlowDuration, totalForwardPackets,
                            avgFlowDuration, avgForwardPackets, stdDevFlowDuration, stdDevForwardPackets));
                }
            }
        }
    }
}