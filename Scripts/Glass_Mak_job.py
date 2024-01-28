import sys
from awsglue.transforms import *
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, input_file_name, split, expr, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

## @params: [JOB_NAME, S3_BUCKET_NAME, S3_FILE_NAME, S3_ARCHIVE_FOLDER]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET_NAME', 'S3_ARCHIVE_FOLDER', 'JDBC_URL', 'DB_USER', 'DB_PASSWORD'])
job_name = args['JOB_NAME']
job_run_id = args['JOB_RUN_ID']
s3_bucket = args['S3_BUCKET_NAME']
s3_archive_folder = args['S3_ARCHIVE_FOLDER']
jdbc_url = args['JDBC_URL']
db_user = args['DB_USER']
db_password = args['DB_PASSWORD']

# Initialize Spark context
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
    "Eventobject": "GLS_MAK"
}

# Define the JDBC URL and other database properties here
db_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# S3 bucket name and file name filter
filter_prefix = "Glasses"  # Customize this to match your file naming pattern
file_extension = "MAK"    # Specify the file extension you want to read

# Check if files exist with the specified extension in the S3 bucket
s3 = boto3.client('s3')
response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=f"{filter_prefix}/")

objects_found = False
if 'Contents' in response:
    for obj in response['Contents']:
        if obj['Key'].endswith(f".{file_extension}"):
            objects_found = True
            break

try:
    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("S3ToPostgres") \
        .getOrCreate()
    
    # Create a DataFrame to read files from S3
    s3_path = f"s3://{s3_bucket}/{filter_prefix}/*.{file_extension}"
    df = spark.read.option("header", "true").csv(s3_path)
    
    # # Add a new column with the S3 file path
    # df = df.withColumn("s3_file_path", input_file_name().cast(StringType()))
    
    # # Trim the first 3 characters from the file name
    #df = df.withColumn("glassdatacode", expr("substring(input_file_name(), 29, 3)"))
    
    # Define code to skip the first 2 rows of a DataFrame
    window_spec = Window.partitionBy(input_file_name()).orderBy(F.monotonically_increasing_id())

    # Add a row number column to the DataFrame within each file
    df = df.withColumn("row_number", F.row_number().over(window_spec))
    
    # Filter out the first 2 rows for each file
    df = df.filter(df.row_number > 2).drop("row_number")
    
    # # Rename columns name CD Description
    df = df.withColumnRenamed("CD  DESCRIPTION                   ", "CD_DESCRIPTION")
    
    # Splitting the data based on the '#' character
    df = df.withColumn("CODE", expr("substring(CD_DESCRIPTION, 1, 4)"))
    df = df.withColumn("DESCRIPTION", expr("substring(CD_DESCRIPTION, 5)"))
    
    ## Show the updated DataFrame
    New_df = df.select("CODE", "DESCRIPTION")#, "glassdatacode")

    # Add new columns createat, createdby, updated, updatedby with values
    New_df = New_df.withColumn("isactive", lit("Y"))
    New_df = New_df.withColumn("createdby", lit("AWS_Glue_User"))
    New_df = New_df.withColumn("createdon", lit(datetime.utcnow()))

    # Update the column names to match the DataFrame schema
    column_mappings = {
        "CODE":"Code",
        "DESCRIPTION":"description",
        "isactive":"isactive",
        "createdby":"createdby",
        "createdon":"createdon"
    }
    
    # Rename the DataFrame columns to match the specified columns
    dataframe = New_df.select(list(column_mappings.keys())).toDF(*column_mappings.values())

    # Count number of rows
    no_rows = dataframe.count()

    # Step 4: Write data to PostgreSQL using JDBC
    dataframe.write.jdbc(url=jdbc_url, table="dbo.gls_mak", mode="append", properties=db_properties)

    # Define status and descriptions
    log_data["status"] = "Succeeded"
    log_data["Description"] = f"Job completed successfully, Inserted Rows count: {no_rows}"

except Exception as e:
    if objects_found:
        # Update log_data for failure
        log_data["Description"] = f"Job failed with error: {e}"
        log_data["status"] = "Failed"
        
    else:
        # Update log_data for failure if no files are found
        log_data["Description"] = "File not Present or invalid file name."
        log_data["status"] = "Failed"

finally:
    # Write log_data to a separate log table
    log_df = spark.createDataFrame([log_data])
    log_df.write.jdbc(url=jdbc_url, table="dbo.otmlog", mode="append", properties=db_properties)
    
    
    #Initialize the S3 client
    s3_client = boto3.client('s3')
    objects_to_archive = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=filter_prefix)
    
    # Get the current date and time
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Loop through the objects and move the ones with the specified file extension to the archive folder
    for obj in objects_to_archive.get('Contents', []):
        # Check if the object has the specified file extension
        if obj['Key'].endswith(f'.{file_extension}'):
            # Define the source and destination keys for copying
            source_key = obj['Key']
            
            # Append the current date and time to the destination key
            filename, extension = source_key.split('.')
            destination_key = f"{s3_archive_folder}/{filename}_{current_date}.{extension}"
            
            # Copy the object to the archive folder
            s3_client.copy_object(
                Bucket=s3_bucket,
                CopySource={'Bucket': s3_bucket, 'Key': source_key},
                Key=destination_key
            )
            
            # Delete the object from the source folder
            s3_client.delete_object(Bucket=s3_bucket, Key=source_key)

    # Stop the Spark context
    sc.stop()
    