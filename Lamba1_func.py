import json
import boto3
from datetime import *
def lambda_handler(event, context):
    # TODO implement
    
    import boto3
    from datetime import datetime, timezone

    today = datetime.now()

    s3 = boto3.client('s3')

    objects1 = s3.list_objects_v2(Bucket='workflowwithtwocrawlers',Prefix='Department/')
    objects = s3.list_objects_v2(Bucket='workflowwithtwocrawlers',Prefix='employee/')

    for o in objects1["Contents"]:
        print("timelast",o["LastModified"].date())
        if o["LastModified"].date() == today.date():
            for o1 in objects["Contents"]:
                print("timelast1",o["LastModified"].date())
                if o1["LastModified"].date() == today.date():
                    print("lambda will start 3rd function")
                    invokelam=boto3.client("lambda")
                    response=invokelam.invoke(FunctionName='forglueworkflow',InvocationType="Event")