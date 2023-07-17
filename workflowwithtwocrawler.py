import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import *
from pyspark.sql.types import *
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="workflow_db",
    table_name="Department")
    
    
dynamic_frame1 = glueContext.create_dynamic_frame.from_catalog(
    database="workflow_db",
   table_name="employee")
    
dynamic_frame1=dynamic_frame1.rename_field("EMAIL","EMAIL_ID")


join_dyf = Join.apply(dynamic_frame,dynamic_frame1, 'DEPARTMENT_ID', 'DEPARTMENT_ID')

join_dyf=join_dyf.filter(lambda x:x["SALARY"]>=1000)


join_dyf=join_dyf.coalesce(1)

s3_output_path = "s3://workflowwithtwocrawlers/output/"

glueContext.write_dynamic_frame.from_options(
    frame=join_dyf,
    connection_type="s3",
    connection_options={"path": s3_output_path},
    format="csv")
