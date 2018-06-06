"""
A client for performing parallelzed SSW using AWS Lambda which
also records various metrics, downloads and checks result files as well as create graphs.
"""
lambdaName = r"<AWS Lambda ARN>" #The ARN of the AWS Lambda function
sqsQueueUrl = r"<SQS Queue Url>" #The URL of the AWS SQS Queue
s3ResultsBucket = r"alignment-results" #The bucket name of the AWS S3 Bucket
# verifyResults - Whether ssw alignments from AWS Lambda should be checked against local alignment scores.
# If you enable this you will also need to make sure that the alignment scores are available locally. See more in main().
verifyResults = False 

import lambda_client as lc
import pathlib
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
from scipy.stats import norm
import numpy as np
import os
import boto3
from filecmp import dircmp

def createTasks():
    totalPartitions = 41
    tasks = set()
    for i in range(1, totalPartitions + 1):
        for j in range(i, totalPartitions + 1):
            tasks.add(lc.Task(
                command=[
                    r"/tmp/ssw_test",
                    r"-pl",
                    r"/var/task/proteinPartitions/partition" + str(i) + ".fasta",
                    r"/var/task/proteinPartitions/partition" + str(j) + ".fasta",
                    r"./BLOSUM62",
                    r"-o 10",
                    r"-e 1"
                ],
                name=str(i) + "-" + str(j),
                executableName="ssw_test",
                lambdaFunctionName=lambdaName
                )
            )
    return tasks

def createJob(concurrencyLimit):
    job = lc.Job(
        tasks=createTasks(),
        concurrencyLimit=concurrencyLimit,
        sqsQueueUrl=sqsQueueUrl,
        s3Bucket=s3ResultsBucket
    )
    return job

def downloadResults(s3Bucket, resultsPath):
    pathlib.Path(resultsPath).mkdir(parents=True, exist_ok=True)
    my_bucket = boto3.resource('s3').Bucket(s3Bucket)

    for object in my_bucket.objects.all():
        my_bucket.download_file(object.key, resultsPath + object.key)

def areFoldersDifferent(dir1, dir2):
    dcmp = dircmp(dir1, dir2)
    differences = len(dcmp.diff_files) + len(dcmp.left_only) + len(dcmp.right_only)
    return differences > 0

def verifyResults(basisResultsPath, trialResultsPath, metricsPath):
    with open(metricsPath + "verification.txt", "w") as output_f:
        if areFoldersDifferent(basisResultsPath, trialResultsPath):
            output_f.write("Basis results at [" + basisResultsPath + "] were DIFFERENT (INCORRECT) from trial results in [" + trialResultsPath + "]\n")
        else:
            output_f.write("Basis results at [" + basisResultsPath + "] were IDENTICAL (CORRECT) to trial results in [" + trialResultsPath + "]\n")

def cleanup(s3Bucket, queueUrl):
    my_bucket = boto3.resource('s3').Bucket(s3Bucket)
    s3Client = boto3.client("s3")
    for object in my_bucket.objects.all():
        s3Client.delete_object(
            Bucket=s3Bucket,
            Key=object.key
        )
    sqsClient = boto3.client("sqs") 
    sqsClient.purge_queue(
        QueueUrl=queueUrl
    )

# Returns the file size (KB) for the protein partition with the corresponding partitionNumber
def getPartitionSize(partitionNumber):
    return os.path.getsize(r"./proteinPartitions/partition" + str(partitionNumber) + ".fasta") / 1000.0

def getJobSize(partition1, partition2):
    return getPartitionSize(partition1) * getPartitionSize(partition2)

def recordPerformanceMetrics(internalTimes, externalTimes, concurrencyLimit, metricsPath, totalTime):
    internalCompletionTime = []
    externalCompletionTime = []
    tasks = []
    counter = 1
    totalPartitions = 41
    pathlib.Path(metricsPath).mkdir(parents=True, exist_ok=True)

    taskSizesCold = []
    taskSizesWarm = []
    internalCompletionTimeCold = []
    internalCompletionTimeWarm = []
    externalCompletionTimeCold = []
    externalCompletionTimeWarm = []

    internalRate = []
    externalRate = []

    with open(metricsPath + "completionTimes.csv", "w") as output_f:
        output_f.write("TaskName, TaskNumber, InternalCompletionTime, ExternalCompletionTime, TaskSize\n")
        for i in range(1, totalPartitions + 1):
            for j in range(i, totalPartitions + 1):
                tasks.append(counter)
                taskName = str(i) + "-" + str(j)
                internalTime = internalTimes[taskName][1] - internalTimes[taskName][0]
                externalTime = externalTimes[taskName][1] - externalTimes[taskName][0]
                taskSize = getJobSize(i, j)
                internalRate.append(taskSize / internalTime)
                externalRate.append(taskSize / externalTime)
                if counter <= concurrencyLimit:
                    taskSizesCold.append(taskSize)
                    internalCompletionTimeCold.append(internalTime)
                    externalCompletionTimeCold.append(externalTime)
                else:
                    taskSizesWarm.append(taskSize)
                    internalCompletionTimeWarm.append(internalTime)
                    externalCompletionTimeWarm.append(externalTime)

                internalCompletionTime.append(internalTime)
                externalCompletionTime.append(externalTime)
                output_f.write(taskName + ", " + str(counter) + ", " + str(internalTime) + ", " + str(externalTime) + ", " + str(taskSize) + "\n")
                counter += 1

    with open(metricsPath + "summary.txt", "w") as summary_f:
        summary_f.write("Real Time: " + str(totalTime) + "\n")
        summary_f.write("Total Compute Time (Lambda Time): " + str(sum(internalCompletionTime)) + "\n")
        summary_f.write("Compute Time Mean: " + str(np.mean(internalCompletionTime)) + ", Median: " + str(np.median(internalCompletionTime)) + ", Standard Deviation:" + str(np.std(internalCompletionTime)) + "\n")
        summary_f.write("Total Observed Time (Spin-up + Compute + Queue Message Receive): " + str(sum(externalCompletionTime)) + "\n")
        summary_f.write("Observed Time Mean: " + str(np.mean(externalCompletionTime)) + ", Median: " + str(np.median(externalCompletionTime)) + ", Standard Deviation:" + str(np.std(externalCompletionTime)) + "\n")
        summary_f.write("Compute-Observed Time Missmatches:\n")
        missmatches = []
        for i in range(0, len(internalCompletionTime)):
            if internalCompletionTime[i] > externalCompletionTime[i]:
                missmatches.append("Job " + str(i) + " had " + str(internalCompletionTime[i]) + " compute time vs " ++ str(externalCompletionTime[i]) + " observed time.\n")
        for missmatch in missmatches:
            summary_f.write(missmatch)
        if len(missmatches) == 0:
            summary_f.write("No Missmatches.\n")

    # Task completion times, Compute & Observed (same plot)
    plt.clf()
    plt.plot(tasks, internalCompletionTime, ".", color="#00cc66", markersize=5, label="Compute Time")
    plt.plot(tasks, externalCompletionTime, ".", color="#ff4d4d", markersize=5, label="Observed Time")
    plt.plot(np.unique(tasks), np.poly1d(np.polyfit(tasks, internalCompletionTime, 1))(np.unique(tasks)), color="#007030", linewidth=3.0)
    plt.plot(np.unique(tasks), np.poly1d(np.polyfit(tasks, externalCompletionTime, 1))(np.unique(tasks)), color="#a10000", linewidth=3.0)
    plt.axvline(x=concurrencyLimit, label="Concurrency Limit", linewidth=2.0)
    plt.xlabel("Task Number")
    plt.ylabel("Completion Time (ms)")
    plt.title("Task Completion Times\n(Concurrency Limit = " + str(concurrencyLimit) + ")")
    plt.legend(loc=1, prop={'size': 10}) # Upper-Right
    plt.tight_layout()
    plt.savefig(metricsPath + "CompletionTimes.png")

    # Task completion rate (task size / completion time), Compute & Observed (same plot)
    plt.clf()
    plt.plot(tasks, internalRate, ".", color="#00cc66", markersize=5, label="Compute Task Rate")
    plt.plot(tasks, externalRate, ".", color="#ff4d4d", markersize=5, label="Observed Rate")
    plt.plot(np.unique(tasks), np.poly1d(np.polyfit(tasks, internalRate, 1))(np.unique(tasks)), color="#007030", linewidth=3.0)
    plt.plot(np.unique(tasks), np.poly1d(np.polyfit(tasks, externalRate, 1))(np.unique(tasks)), color="#a10000", linewidth=3.0)
    plt.axvline(x=concurrencyLimit, label="Concurrency Limit", linewidth=2.0)
    plt.xlabel("Task Number")
    plt.ylabel(r"Rate ($KB^2/ms$)" + "\n")
    plt.title("Rate\nTask Size(Partition 1 FS * Partition 2 FS)/Completion Time\n(Concurrency Limit = " + str(concurrencyLimit) + ")")
    plt.legend(loc=1, prop={'size': 10}) # Upper-Right
    plt.tight_layout()
    plt.savefig(metricsPath + "Rates.png")

    # Probability distribution of Task Observed Completion Times
    plt.clf()
    plt.xlabel("Completion Time (ms)")
    plt.ylabel("Relative Frequency")
    plt.title("Task Observed Times\n(Concurrency Limit = " + str(concurrencyLimit) + ")")
    plt.hist(externalCompletionTime, 50, normed=1, facecolor='green', alpha=0.70)
    x = np.linspace(norm(np.mean(externalCompletionTime), np.std(externalCompletionTime)).ppf(0.01), norm(np.mean(externalCompletionTime), np.std(externalCompletionTime)).ppf(0.99), 100)
    plt.plot(x, norm(np.mean(externalCompletionTime), np.std(externalCompletionTime)).pdf(x), 'r-', lw=3, alpha=0.5, label='norm pdf')
    plt.text(x[int(len(x)/1.5)], norm(np.mean(externalCompletionTime), np.std(externalCompletionTime)).pdf(x[int(len(x)/1.5)]) * 1.2, r"$N(\mu=" + str(int(round(np.mean(externalCompletionTime)))) + r", \sigma=" + str(int(round(np.std(externalCompletionTime)))) + ")$")
    plt.tight_layout()
    plt.savefig(metricsPath + "ObservedTimesDistribution.png")

    # Probability distribution of Task Compute Completion Times
    plt.clf()
    plt.xlabel("Completion Time (ms)")
    plt.ylabel("Relative Frequency")
    plt.title("Task Compute Completion Times\n(Concurrency Limit = " + str(concurrencyLimit) + ")")
    plt.hist(internalCompletionTime, 50, normed=1, facecolor='green', alpha=0.70)
    x = np.linspace(norm(np.mean(internalCompletionTime), np.std(internalCompletionTime)).ppf(0.01), norm(np.mean(internalCompletionTime), np.std(internalCompletionTime)).ppf(0.99), 100)
    plt.plot(x, norm(np.mean(internalCompletionTime), np.std(internalCompletionTime)).pdf(x), 'r-', lw=3, alpha=0.5, label='norm pdf')
    plt.text(x[int(len(x)/1.5)], norm(np.mean(internalCompletionTime), np.std(internalCompletionTime)).pdf(x[int(len(x)/1.5)]) * 1.2, r"$N(\mu=" + str(int(round(np.mean(internalCompletionTime)))) + r", \sigma=" + str(int(round(np.std(internalCompletionTime)))) + ")$")
    plt.tight_layout()
    plt.savefig(metricsPath + "ComputeTimesDistribution.png")

    # # Task Compute Completion time VS task size (partiton1 file size * partition 2 file size)
    # plt.clf()
    # plt.xlabel("Task Size\n(Partition 1 File Size * Partition 2 File Size)")
    # plt.ylabel("Task Completion Time (ms)")
    # plt.title("Completion Time vs Task Size\n(Concurrency Limit = " + str(concurrencyLimit) + ")")
    # plt.xlabel("Task Size " + r"($KB^2$)" + "\n(Partition 1 File Size * Partition 2 File Size)\n")
    # plt.plot(taskSizesCold, internalCompletionTimeCold, "x", color="#3030B0", label="Compute (Cold-Start) Time vs Task Size")
    # plt.plot(taskSizesWarm, internalCompletionTimeWarm, "x", color="#B03030", label="Compute (Warm-Start) Time vs Task Size")
    # plt.plot(np.unique(taskSizesCold), np.poly1d(np.polyfit(taskSizesCold, internalCompletionTimeCold, 1))(np.unique(taskSizesCold)), color="#3030B0")
    # plt.plot(np.unique(taskSizesWarm), np.poly1d(np.polyfit(taskSizesWarm, internalCompletionTimeWarm, 1))(np.unique(taskSizesWarm)), color="#B03030")
    # plt.legend(loc=4, prop={'size': 10})
    # plt.tight_layout()
    # plt.savefig(metricsPath + "ComputeTimeVsTaskSize.png")

    # # Task Observed Completion time VS task size (partiton1 file size * partition 2 file size)
    # plt.clf()
    # plt.xlabel("Task Size " + r"($KB^2$)" + "\n(Partition 1 File Size * Partition 2 File Size)\n")
    # plt.ylabel("Task Completion Time (ms)")
    # plt.title("Completion Time vs Task Size\n(Concurrency Limit = " + str(concurrencyLimit) + ")")
    # plt.plot(taskSizesCold, externalCompletionTimeCold, "x", color="#3030B0", label="Observed (Cold-Start) Time vs Task Size")
    # plt.plot(taskSizesWarm, externalCompletionTimeWarm, "x", color="#B03030", label="Observed (Warm-Start) Time vs Task Size")
    # plt.plot(np.unique(taskSizesCold), np.poly1d(np.polyfit(taskSizesCold, externalCompletionTimeCold, 1))(np.unique(taskSizesCold)), color="#3030B0")
    # plt.plot(np.unique(taskSizesWarm), np.poly1d(np.polyfit(taskSizesWarm, externalCompletionTimeWarm, 1))(np.unique(taskSizesWarm)), color="#B03030")
    # plt.legend(loc=4, prop={'size': 10})
    # plt.tight_layout()
    # plt.savefig(metricsPath + "ObservedTimeVsTaskSize.png")

def main():
    for i in range(0, 3):
        trialNumber = 1 + i
        print("Starting trial", trialNumber)

        concurrencyLimit = 1000
        job = createJob(concurrencyLimit)
        job.executeAllTasks()
        totalTime, internalTimes, externalTimes = job.getTasksTimes()
        
        path = r"./performanceData/concurrency" + str(concurrencyLimit) + "/trial" + str(trialNumber) + "/"
        recordPerformanceMetrics(internalTimes, externalTimes, concurrencyLimit, path, totalTime)

        # To verify results you will first need to obtain the SSW alignments by running SSW locally.
        # The resulting alignments need to be placed in ./results_basis.
        # You can obtain SSW alignments by running the scripts inside ./examples/proteinSequenceAlignment/preprocessing/
        if verifyResults:
	        trialResultsDir = path + "results/"
	        basisResultsDir = "./results_basis/"
	        downloadResults(s3ResultsBucket, trialResultsDir)
	        verifyResults(basisResultsDir, trialResultsDir, path)
        cleanup(s3ResultsBucket, sqsQueueUrl)
    print("Done")

main()