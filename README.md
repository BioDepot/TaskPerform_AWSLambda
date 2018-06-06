This repository contains code for performing general computational tasks in parallel using AWS Lambda as well as a specific example for perfoming protein sequence alignment using SW (Smith-Waterman's algorithm).

### TaskPerform
The generalized task performing code is located in taskPerform. It consists of two files:
* lambda_function.py - The python code which is executed upon launching an AWS Lambda function. This file includes the AWS Lambda function's handler (called handler(event, context)) which is the starting point of the Lambda function. The lambda_function.py needs to be packaged together with the executable which will perform the desired task (a .bash script, an executable, etc.) and possibly any data needed. This package is to be zipped and uploaded to an AWS Lambda function.
* lambda_client.py - This file will typically be located on your laptop or EC2 instance and manage the execution of your tasks. The file contains code for creating the tasks which includes specifying the command each for each task (i.e. myScript.sh -file3.data -file5.data), launching the AWS Lambda functions to perform these tasks and scheduling their execution (keeping the number of simultenously running functions under a desired limit).

### SequenceAlignment Example
The repository also includes an example usage of taskPerform for protein sequence alignment which is located in examples/proteinSequenceAlignment/. The core setup for running a pair-wise protein sequence alignment on the human protein list is:
* Setup AWS credentials/python3 packages - Make sure you can connect to AWS services from python3. See **AWS Credentials/Boto3 Setup bellow**.
* Create a AWS Lambda function - This will be the function that performs each sequence alignment task. You will need to create an IAM Role for your lambda (which specifies permissions your Lambda function has). At minimum you will need to attach a policy for SQS and S3 access. You will also need to set the memory (which the CPU power is proportional to) of the AWS Lambda to an appropriate level (1536 MB was used for testing). The timeout should be increased to maximum (5 min).
* lambdaPackage - this folder needs to be zipped and uploaded to the AWS Lambda function.
* client/minimal_align_client.py - This code can be run from your laptop/pc/EC2 etc.
There are some additional steps needed before you can run minimal_align_client.py:
* Create a AWS SQS Queue - This is used to report the completion of each task on AWS Lambda. You will also have to add a permission to this queue so you can poll/read/delete messages from the Queue. (this is done by the lambda_client.py)
* Create an AWS S3 Bucket - This will be the destination bucket for the result files from the alignment.
* Modify the variables **lambdaName** , **sqsQueueUrl** , **s3ResultsBucket** in minimal_align_client.py to reflect the AWS Lambda, AWS Queue and AWS S3 Bucket you've created.
* Add permissions for the AWS user performing the sequence alignment. You can find these in AWS Console/IAM/Users/Permissions. From here you can attach policies for specific services. Testing was succesful with the following policies: IAMFullAccess, AmazonS3FullAccess, AWSLambdaRole and the following custom inline policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction",
                "lambda:InvokeAsync"
            ],
            "Resource": "*"
        }
    ]
}
```

### Additional information about proteinSequenceAlignment example:
* The human protein list file uniprot_humanProteinList.fasta was obtained from https://www.uniprot.org/uniprot/?query=reviewed%3Ayes+AND+proteome%3Aup000005640 .
* The 500 proteins/file partitions were created using the /preprocessing/partitionProteins.py python code.
* examples/proteinSequenceAlignment/client also includes a metrics_align_client.py which creates a more detailed report, benchmarking completion times, downloading and checking the results.
* The SSW used for the sequence alignment was modified from the original. The modified source code is located in examples/proteinSequenceAlignment/ssw. The only file changed was the main.c. The change consists of adding a -l flag which makes ssw_test only output a single number and a coma for each sequence alignment (representing the alignment score). This makes it easy to capture the output of ssw_test directly.


### Troubleshooting SequenceAlignment:
The following may be helpful for debugging/troubleshooting the sequence alignment example:
* AccessDenied - Check AWS Permissions/Roles/Policies

You can also do a small scale test, once you have setup your Lambda, S3 and SQS by running a single Lambda function directly from the AWS Console. The following is a raw json which can serve as a test event:

```json
{
  "taskName": "7-17",
  "executableName": "ssw_test",
  "command": [
    "/tmp/ssw_test",
    "-pl",
    "/var/task/proteinPartitions/partition7.fasta",
    "/var/task/proteinPartitions/partition17.fasta",
    "./BLOSUM62",
    "-o 10",
    "-e 1"
  ],
  "sqsQueueUrl": "<QUEUE URL GOES HERE>",
  "s3Bucket": "<S3 BUCKET NAME GOES HERE>"
}
```
Upon completion you should notice a new message appear in the SQS with the given url and a single result file in the S3 bucket. Make sure that the SQS Queue is completely empty (you can purge it from the AWS Console) before you run the full-scale sequence alignment.

### AWS Credentials/Boto3 Setup:
* Install boto3 for python (pip install boto3). See http://boto3.readthedocs.io/en/latest/guide/quickstart.html
* Configure AWS credentials & region:
    * Create a User and from it create an access key - access secret pair. You will also need to add Policies to the User you've created that allow at least SQS Queue access. See https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey
    * Configure your machine so the code uses the key - secret pair you created. The easiest way is to create a file with the structure:
```
[default]
aws_access_key_id=YOUR_ACCESS_KEY
aws_secret_access_key=YOUR_SECRET_KEY
```
with file path `~/.aws/credentials`. Where the access key & secret key are created from an IAM User. See http://boto3.readthedocs.io/en/latest/guide/quickstart.html
    * Set your region. Easiest way is to create a file with the structure:
```
region=us-west-2
```
with file path `~/.aws/config`. See http://boto3.readthedocs.io/en/latest/guide/quickstart.html
* Setup SQS Queues:
    * Create 2 SQS Standard Queues from AWS console; one for AlignmentTasks and one for AlignmentResults
    * Add permissions to your SQS Queues so they can receive messages. For ease I just added "Everybody (*) All SQS Actions (SQS:*)" permissions.
    * Change the code to use the queues URLs of the queues you just created.


### Acknowledgements
* Test Data obtained from https://www.uniprot.org/uniprot/?query=reviewed%3Ayes+AND+proteome%3Aup000005640 .
* The code for performing SW is due to Mengyao Zhao , Wan-Ping Lee, Erik P. Garrison, Gabor T. Marth. The original source code can be found at https://github.com/mengyao/Complete-Striped-Smith-Waterman-Library .