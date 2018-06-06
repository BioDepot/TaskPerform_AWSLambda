"""
Contains the code which needs to be uploaded to AWS Lambda.
The entry point of the Lambda needs to be the handler(event, context) function contained here.
"""
from pathlib import Path
from shutil import copyfile
import stat
import os
import subprocess
import boto3
from boto3.s3.transfer import S3Transfer
import time
import json

# Prepares the executable/script for execution by copying into /tmp/ folder and
# adding execution permission.
def prepareExecutable(executableName):
    destination = "/tmp/" + executableName
    if Path(destination).is_file():
        # File is already there - no need to copy
        pass
    else:
        source = "./" + executableName
        copyfile(source, destination)
    if not os.access(destination, os.X_OK):
        st = os.stat(destination)
        os.chmod(destination, st.st_mode | stat.S_IEXEC)

# Peforms the tasks specified by the command and captures the output in file resultFileName. The file is then uploaded to the s3Bucket
def performTask(command, resultFileName, s3Bucket):
    with open(r"/tmp/" + resultFileName, "w") as output_f:
        p = subprocess.Popen(command, stdin=None, stdout=output_f, stderr=subprocess.PIPE, universal_newlines=True)
        (stdout_text, stderr_text) = p.communicate()
        print("stderr_text = ", stderr_text)
    # Uploaded Results
    transfer = S3Transfer(boto3.client("s3"))
    transfer.upload_file(r"/tmp/" + resultFileName, s3Bucket, resultFileName)

# Marks this job complete by sending a SQS message.
def markComplete(taskName, startTime, queueUrl):
    sqs = boto3.client('sqs')
    endTime = int(round(time.time() * 1000))

    response = sqs.send_message(
        QueueUrl=queueUrl,
        DelaySeconds=0,
        MessageAttributes={
            "TaskName": {
                "DataType": "String",
                "StringValue": taskName
            },
            "StartTime": {
                "DataType": "Number",
                "StringValue": str(startTime)
            },
            "EndTime": {
                "DataType": "Number",
                "StringValue": str(endTime)
            }
        },
        MessageBody=(
            "Task " + taskName + " took a total of " + str((endTime - startTime) / 1000.0) + " seconds to complete."
        )
    )

def handler(event, context):
    startTime = int(round(time.time() * 1000))

    print(json.dumps(event, indent=4, sort_keys=True))
    print(event["command"])

    prepareExecutable(event["executableName"])
    performTask(event["command"], event["taskName"], event["s3Bucket"])
    markComplete(event["taskName"], startTime, event["sqsQueueUrl"])

    return 0