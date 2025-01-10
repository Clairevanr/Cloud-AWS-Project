package fr.emse;

import java.nio.file.Path;
import software.amazon.awssdk.regions.Region;

public class Characteristics {

    // Variables fixes pour AWS S3 et autres
    public static String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/330112968061/messaging-app-queue";
    public static String SQS_SUMMARIZE = "https://sqs.us-east-1.amazonaws.com/330112968061/SQS_Summarize";
    public static String SQS_CONSOLIDATOR= "https://sqs.us-east-1.amazonaws.com/330112968061/SQS_Worker";


    public static final String SOURCE_BUCKET = "project-source-bucket-2025"; 
    public static final String TEMP_BUCKET= "project-temp-bucket-2025";
    public static final String FINAL_BUCKET= "project-final-bucket-2025";


    public static final Region REGION = Region.US_EAST_1;
    public static final Path DATA_REPOSITORY = Path.of("data");
    public static final Path SUMMARY_FILE_PATH = Path.of("data/summaries/summary.csv");
    public static final Path OUTPUT_FILE_PATH = Path.of("data/stats/stats.csv");

    // Fichier à upload 

    public static final String FILENAME = "data-20221202.csv";
    
}
