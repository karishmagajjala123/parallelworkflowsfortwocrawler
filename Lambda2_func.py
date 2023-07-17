import json
import boto3

client=boto3.client("glue")

def lambda_handler(event, context):
    
    response = client.start_workflow_run(Name='parallelworkflowsfortwocrawler')
    