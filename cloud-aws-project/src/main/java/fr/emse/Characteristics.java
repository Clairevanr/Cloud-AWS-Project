package fr.emse;

import java.nio.file.Path;
import software.amazon.awssdk.regions.Region;

public class Characteristics {

    // Variables fixes pour AWS S3 et autres
    public static String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/330112968061/messaging-app-queue";
    public static final String BUCKET_NAME = "myprojectbucket355552555"; 
    public static final Region REGION = Region.US_EAST_1;
    public static final Path DATA_REPOSITORY = Path.of("data");
    public static final Path SUMMARY_FILE_PATH = Path.of("data/summaries/summary.csv");
    public static final Path OUTPUT_FILE_PATH = Path.of("data/traffic_stats/stats.csv");
    
}
