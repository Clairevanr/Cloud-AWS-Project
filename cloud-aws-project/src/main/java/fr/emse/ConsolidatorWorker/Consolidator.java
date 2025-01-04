package fr.emse.ConsolidatorWorker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;




public class Consolidator {

    public static void main(String[] args) throws IOException {
        
        String filePath = "/home/clairevanruymbeke/Cloud-AWS-Final-project/data/summaries/summary.csv";
        

        

          // Open the summary file and read data
          try (BufferedReader summarizedReader = Files.newBufferedReader(Path.of(filePath))) {
            consolidateCSVData(summarizedReader);
        } catch (IOException e) {
            System.out.println("Error reading the file: " + e.getMessage());
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

        // Define the output file path
        Path outputPath = Path.of("data/traffic_stats/stats.csv");

        // Create the directory if it doesn't exist
        Files.createDirectories(outputPath.getParent());

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
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
