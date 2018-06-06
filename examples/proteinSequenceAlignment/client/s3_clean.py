"""
Deletes all files in the the bucket with the specified name.
This assumes that there are no folders in the specified bucket.
"""

import os
import boto3
import threading

bucket = "alignment-results" # The S3 bucket to delete all files in.
my_bucket = boto3.resource('s3').Bucket(bucket)

for object in my_bucket.objects.all():
    boto3.client("s3").delete_object(
        Bucket=bucket,
        Key=object.key
    )
