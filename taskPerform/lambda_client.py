"""
Contains code for the client which sets up the tasks and launches Lambdas.
"""
import json
import boto3
import time
import threading

class Task:
    """
    A single task to be executed on Lambda.

    Attributes:
        command: A list of strings representing the command to be run on Lambda. Ex.: ["/tmp/myScript.exe", "arg1", "arg2"]. Notice that myScript.exe is expected to be located in /tmp/ as a result of moving the executable to the /tmp/ folder from where it can be launched in Lambda.
        name: A string representing an unique identifier for the task. This name will also be used as the file name of the results in S3 so the name must be a valid S3 file name.
        executableName: The name of the executable file that is being run on lambda. This parameter is needed since the executable cannot be run directly in Lambda's environment; each Lambda will copy the executable to /tmp/ and add executable permissions to run it.
    """
    def __init__(self, command, name, executableName, lambdaFunctionName):
        if type(command) is not list:
            raise TypeError("Command should be a list of strings.")
        self.command = command
        self.name = name
        self.executableName = executableName
        self.lambdaFunctionName = lambdaFunctionName
        self.__lambdaClient = boto3.client('lambda')


    def start(self, queueUrl, s3Bucket):
        t = threading.Thread(target=self.__invocationWorker, args=(queueUrl, s3Bucket, ))
        t.start()

    def __invocationWorker(self, queueUrl, s3Bucket):
        self.__lambdaClient.invoke(
            FunctionName=self.lambdaFunctionName,
            InvocationType="Event", #Async
            Payload=json.dumps({
                "taskName": self.name,
                "executableName": self.executableName,
                "command": self.command,
                "sqsQueueUrl": queueUrl,
                "s3Bucket": s3Bucket
            })
        )

class Job:
    """
    A job consisting of a set of tasks to be executed using Lambda.

    Attributes:
        tasks: A set of Tasks which define this job.
        concurrencyLimit: Maximum number of Lambdas to be run at any one time.
        sqsQueueUrl: The url of the SQS Queue being used for reporting finished Lambda tasks.
        s3Bucket: The S3 bucket for storing the results.
    """
    def __init__(self, tasks, concurrencyLimit, sqsQueueUrl, s3Bucket):
        if type(tasks) is not set:
            raise TypeError("tasks should be a set of Tasks.")
        self.tasks = tasks
        self.concurrencyLimit = concurrencyLimit
        self.queueUrl = sqsQueueUrl
        self.s3Bucket = s3Bucket
        self.__sqsClient = boto3.client("sqs")
        self.__taskTimesInternal = {} #task.name - [startTime, endTime]. This is a measurement of the compute time inside lambda
        self.__taskTimesExternal = {} #task.name - [startTime, endTime]. This is a measurement of the total time it took for this task, including starting it up, lambda compute time and the time it took to collect its message from SQS
        self.__taskMessages = {} #task.name - message body (SQS message)
        for task in self.tasks:
            self.__taskTimesInternal[task.name] = [0, 0]
            self.__taskTimesExternal[task.name] = [0, 0]
            self.__taskMessages[task.name] = "No message received."
        self.__concurrentTasksCount = 0
        self.__completedTasks = 0

    def __getTimeMs(self):
        return int(round(time.time() * 1000))

    def __startNextTask(self):
        nextTask = self.tasks.pop()
        self.__taskTimesExternal[nextTask.name][0] = self.__getTimeMs()
        self.__concurrentTasksCount += 1
        nextTask.start(self.queueUrl, self.s3Bucket)

    def executeAllTasks(self):
        """
        Executes all the Tasks in this Job.
        """
        startTime = self.__getTimeMs()

        taskMessages = []
        totalTasks = len(self.tasks)
        
        # Start initial min(totalTasks, self.concurrencyLimit) number of tasks.
        for i in range(0, min(totalTasks, self.concurrencyLimit)):
            self.__startNextTask()

        # Poll SQS until the finished message of each task is collected. Any time a finished message is received, it is deleted and a new task is started.
        while self.__completedTasks < totalTasks:
            response = self.__sqsClient.receive_message(
                QueueUrl=self.queueUrl,
                MessageAttributeNames=[
                    "TaskName",
                    "StartTime",
                    "EndTime"
                ],
                MaxNumberOfMessages=10, #between 1 and 10
                VisibilityTimeout=5
            )
            #print(json.dumps(response, indent=4, sort_keys=True))
            if "Messages" in response:
                for msg in response["Messages"]:
                    taskName = msg["MessageAttributes"]["TaskName"]["StringValue"]

                    self.__taskMessages[taskName] = msg["Body"]
                    self.__taskTimesExternal[taskName][1] = self.__getTimeMs()
                    self.__taskTimesInternal[taskName] = (int(msg["MessageAttributes"]["StartTime"]["StringValue"]), int(msg["MessageAttributes"]["EndTime"]["StringValue"]))
                    self.__completedTasks += 1
                    self.__concurrentTasksCount -= 1
                    self.__sqsClient.delete_message(
                        QueueUrl=self.queueUrl,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                for i in range(0, min(len(self.tasks), self.concurrencyLimit - self.__concurrentTasksCount)):
                    self.__startNextTask()
            time.sleep(0.2) #Wait 200 ms before polling SQS again

        endTime = self.__getTimeMs()

        self.__totalTime = endTime - startTime

    def getTasksTimes(self):
        return (self.__totalTime, self.__taskTimesInternal, self.__taskTimesExternal)