package fr.emse.ConsolidatorWorker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;

import software.amazon.awssdk.core.ResponseInputStream;



public class Consolidator {

    public static void main(String[] args) throws IOException {
        Region region = Region.US_EAST_1;
        String fileName = "summary.csv";
        String bucketName = "myprojectbucket355552555";

        S3Client s3 = S3Client.builder().region(region).build();

        // Check if the file exists
        ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucketName).build();
        ListObjectsResponse res = s3.listObjects(listObjects);
        List<S3Object> objects = res.contents();

        if (objects.stream().anyMatch((S3Object x) -> x.key().equals(fileName))) {
            GetObjectRequest objRequest = GetObjectRequest.builder().key(fileName).bucket(bucketName).build();
            ResponseInputStream<GetObjectResponse> oldData = s3.getObject(objRequest);
            BufferedReader summarizedReader = new BufferedReader(new InputStreamReader(oldData));

            consolidateCSVData(summarizedReader);
        } else {
            System.out.println("The file doesn't exist");
        }
    }

    private static void consolidateCSVData(BufferedReader summarizedReader) throws IOException {
        // Maps to store data by Src IP -> Dst IP -> List of Flow Durations and Forward Packets
        Map<String, Map<String, List<Long>>> trafficDataFlowDuration = new HashMap<>();
        Map<String, Map<String, List<Integer>>> trafficDataForwardPackets = new HashMap<>();

        // Skip the header line
        String line = summarizedReader.readLine(); // Read and ignore the header

        // Read the file
        while ((line = summarizedReader.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length < 5){

            
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

    // Write the statistics to a csv file
    private static void writeTrafficStatsToCSV(Map<String, Map<String, List<Long>>> trafficDataFlowDuration,
                                               Map<String, Map<String, List<Integer>>> trafficDataForwardPackets) throws IOException {

        // Define the output file path
        Path outputPath = Path.of("data/traffic_stats/stats.csv");


        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            // Write the header row
            writer.write("Src IP,Dst IP,Avg Flow Duration,Avg Forward Packets,Std Dev Flow Duration,Std Dev Forward Packets\n");

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

                    // Write the results to the CSV file
                    writer.write(String.format("%s,%s,%.2f,%.2f,%.2f,%.2f\n", 
                            srcIP, dstIP, avgFlowDuration, avgForwardPackets, stdDevFlowDuration, stdDevForwardPackets));
                }
            }
        }
    }
}
