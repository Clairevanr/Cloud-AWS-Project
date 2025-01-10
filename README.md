# cloud-aws-project To make the project run : 

- Clone the git repository 
- Connect on your Amazon AWS account and update your credentials 
- Open the repository in your favourite IDE (everything was developped using VS Code) To set up Amazon AWS : - Create a s3 bucket 
- Update the Characteristics File in the project's root directory with your own environment variables (Especially check the relative local paths are accurate to make everything run smoothly)

If needed, create your own SQS queues (there are two) and your own three s3 Buckets. You will find every details in the Characteristics file. 


# How to configurate your AWS environment 

## SQS queues :

You will need two SQS queues : one to trigger the lambda function Summarize when the client updates the file (SQS_SUMMARIZE) and one to trigger the lambda function Consolidator (SQS_CONSOLIDATOR).

## S3 Buckets 

You will need three s3 buckets : a SOURCE_BUCKET, a TEMP_BUCKET and a FINAL_BUCKET.

## Lambda functions

You will need two lambda functions :

- One to Summarize, with SQS_SUMMARIZE as a trigger and SQS_CONSOLIDATOR as a destinations
- One to Consolidate, with SQS_CONSOLIDATOR as a trigger

To upload the code on Lambda, package the app in the aws-cloud-project directory with :
    
    mvn package 

You can then modify the Characteristics files consequently.

# Test the capabilities 

To use the system, 
You can start by uploading the file of your choice in the SOURCE_BUCKET thanks to the Upload_Client.

- Then if, you want to use the Lambda version, you can configure your lambda functions and let them do the work.
- If you want to use the java app version, which we failed to launch in a EC2 instance, you can run the SummarizeWorker and the ConsolidatorWorker successively after using the Upload_Client

The Export client is integrated in the ConsolidatorWorker.




