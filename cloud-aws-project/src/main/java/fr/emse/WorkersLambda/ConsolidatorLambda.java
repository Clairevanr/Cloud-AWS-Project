package fr.emse.WorkersLambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;

import fr.emse.Client.Upload_Client;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static fr.emse.Characteristics.*;


public class ConsolidatorLambda implements RequestHandler<SQSEvent, String> {

    private static final String TEMP_BUCKET = "temp-bucket-name";
    //private static final String FINAL_BUCKET = "final-bucket-name";
    private static final String SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/SQS_Consolidator";
    private static final Path OUTPUT_FILE_PATH = Paths.get("/tmp/stats.csv");

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        SqsClient sqsClient = SqsClient.create();
        S3Client s3Client = S3Client.create();

        for (SQSEvent.SQSMessage message : event.getRecords()) {
            String fileName = extractFileNameFromMessage(message.getBody());

            ResponseInputStream<GetObjectResponse> s3ObjectStream = s3Client.getObject(GetObjectRequest.builder()
                            .bucket(FINAL_BUCKET)
                            .key(fileName)
                            .build());

            processSummarizedData(s3Client, s3ObjectStream, FINAL_BUCKET);
            Upload_Client.deleteFileFromS3(s3Client, FINAL_BUCKET, fileName);

            System.out.println("Consolidated data for file '" + fileName + "'");
        }

        sqsClient.close();
        s3Client.close();
        return "Processing Complete";
    }


    ///////
    public static void processSummarizedData(S3Client s3, ResponseInputStream<GetObjectResponse> data, String bucketName) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(data, StandardCharsets.UTF_8))) {
            Map<String, TrafficStats> trafficStatsMap = new HashMap<>();

            // Read header
            String header = reader.readLine();
            if (header == null) {
                throw new IllegalArgumentException("The summarized data file is empty.");
            }

            // Process each row
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = line.split("\\t"); // Assuming tab-separated values
                if (columns.length < 5) continue; // Skip invalid rows

                String srcIp = columns[0];
                String dstIp = columns[1];
                String key = srcIp + "-" + dstIp;

                long totalFlowDuration = Long.parseLong(columns[3]);
                long totalForwardPackets = Long.parseLong(columns[4]);

                // Aggregate data for average and standard deviation calculations
                trafficStatsMap.computeIfAbsent(key, k -> new TrafficStats(srcIp, dstIp))
                               .add(totalFlowDuration, totalForwardPackets);
            }

            // Finalize calculations for all pairs
            trafficStatsMap.values().forEach(TrafficStats::finalizeStats);

            // Prepare consolidated output
            String output = "Src IP\tDst IP\tAvg Flow Duration\tStd Dev Flow Duration\tAvg Forward Packets\tStd Dev Forward Packets\n";
            output += trafficStatsMap.values().stream()
                                     .map(TrafficStats::toString)
                                     .collect(Collectors.joining("\n"));

            // Upload consolidated results to S3
            String outputKey = "consolidated-traffic-stats.csv";
            s3.putObject(builder -> builder.bucket(bucketName).key(outputKey).build(),
                    RequestBody.fromString(output));

            System.out.println("Consolidated data uploaded successfully to S3: " + outputKey);

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error processing the summarized data file: " + e.getMessage(), e);
        }
    }

    // Helper class to calculate statistics
    private static class TrafficStats {
        private final String srcIp;
        private final String dstIp;
        private final List<Long> flowDurations;
        private final List<Long> forwardPackets;

        private double avgFlowDuration;
        private double stdDevFlowDuration;
        private double avgForwardPackets;
        private double stdDevForwardPackets;

        public TrafficStats(String srcIp, String dstIp) {
            this.srcIp = srcIp;
            this.dstIp = dstIp;
            this.flowDurations = new ArrayList<>();
            this.forwardPackets = new ArrayList<>();
        }

        public void add(long flowDuration, long forwardPackets) {
            this.flowDurations.add(flowDuration);
            this.forwardPackets.add(forwardPackets);
        }

        public void finalizeStats() {
            this.avgFlowDuration = calculateAverage(flowDurations);
            this.stdDevFlowDuration = calculateStdDev(flowDurations, avgFlowDuration);
            this.avgForwardPackets = calculateAverage(forwardPackets);
            this.stdDevForwardPackets = calculateStdDev(forwardPackets, avgForwardPackets);
        }

        private double calculateAverage(List<Long> values) {
            return values.stream().mapToDouble(Long::doubleValue).average().orElse(0.0);
        }

        private double calculateStdDev(List<Long> values, double mean) {
            return Math.sqrt(values.stream()
                                   .mapToDouble(val -> Math.pow(val - mean, 2))
                                   .average()
                                   .orElse(0.0));
        }

        @Override
        public String toString() {
            return srcIp + "\t" + dstIp + "\t" +
                   String.format("%.2f", avgFlowDuration) + "\t" +
                   String.format("%.2f", stdDevFlowDuration) + "\t" +
                   String.format("%.2f", avgForwardPackets) + "\t" +
                   String.format("%.2f", stdDevForwardPackets);
        }
    }

    private String extractFileNameFromMessage(String messageBody) {
        return messageBody.substring(messageBody.indexOf(":") + 2, messageBody.indexOf(" in bucket:")).trim();
    }





    
}