import sys
from awsglue.transforms import *
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from config import *
from config import archive

## @params: [JOB_NAME, S3_BUCKET_NAME, S3_FILE_NAME, S3_ARCHIVE_FOLDER]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_FILE_NAME'])
job_name = args['JOB_NAME']
job_run_id = args['JOB_RUN_ID']
s3_file_name= args['S3_FILE_NAME']
s3_bucket = S3_BUCKET_NAME
s3_archive_folder = S3_ARCHIVE_FOLDER
jdbc_url = JDBC_URL
db_user = DB_USER
db_password = DB_PASSWORD

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Capture process information
log_data = {
    "jobid": job_run_id,
    "pipelinename": job_name,
    "Description": "Job started",
    "status": "Running",
    "Eventdate": datetime.utcnow(),
    "Eventuser": "Otm_User",
}

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("S3ToPostgres") \
    .getOrCreate()
    
# Define the JDBC URL and other database properties here
db_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

try:
    # Check if the S3 file exists
    s3_path = f"s3://{s3_bucket}/{args['S3_FILE_NAME']}"
    s3_client = boto3.client('s3')
    s3_object = s3_client.head_object(Bucket=s3_bucket, Key=s3_file_name)
    
    if s3_object:
        
        # Step 2: Read Data from S3 using dynamic bucket and file names
        df = spark.read.csv(s3_path, header=True, inferSchema=True)
        
        # #Add new columns createat,createdby,updated,updatedby with values
        df = df.withColumn("isactive", lit("Y"))
        df = df.withColumn("createdby", lit("AWS_Glue_User"))
        df = df.withColumn("createdon", lit(datetime.utcnow()))
        # df = df.withColumn("updatedby", lit("AWS_Glue_User"))
        # df = df.withColumn("updatedon", lit(datetime.utcnow()))
        
        # Cast the "IntegerType" columns to IntegerType
        df = df.withColumn("startyear", df["startyear"].cast(IntegerType()))
        df = df.withColumn("latestyear", df["latestyear"].cast(IntegerType()))
        
        # Update the column names to match the DataFrame schema
        columns = ["MakeCode", "description", "startyear", "latestyear", "isactive", "createdby", "createdon"]
        
        # Rename the DataFrame columns to match the specified columns
        df = df.toDF(*columns)
        
        #count number of rows
        no_rows = df.count()
        
        #step 4: write data to  postgres JDBC
        df.write.jdbc(url=jdbc_url, table="dbo.vehiclemake", mode="append", properties=db_properties)

        #Define status and Descriptions
        log_data["status"] = "Succeeded"
        log_data["Description"] = "Job completed successfully, Inserted Rows count:"+str(no_rows)
        
    else:
        # If the S3 file doesn't exist, set the job as failed
        log_data["Description"] = "File not Present or invalid file name."
        log_data["status"] = "Failed"
    
except Exception as e:
        # Handle other errors and update the job status and description
        log_data["Description"] = f"Job failed with error: {e}"
        log_data["status"] = "Failed"

finally:
    # Write log_data to a separate log table
    log_df = spark.createDataFrame([log_data])
    log_df.write.jdbc(url=jdbc_url, table="dbo.otmlog", mode="append", properties=db_properties)

    # functions for file move to archive
    archive(s3_file_name)

    # Stop the Spark context
    sc.stop()