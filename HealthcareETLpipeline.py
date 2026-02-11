from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, from_csv, lag, round, stddev, unix_timestamp, lit, to_date, sha2, lower, trim, datediff
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DecimalType
import google.cloud.logging
import logging

# Setup variables 
BUCKET_NAME = "healthcare_bucket_data"
GCS_BUCKET_SOURCE = f"gs://{BUCKET_NAME}/healthcare_dataset.csv"
BQ_TABLE = "project-a2f9a359-1d5f-40ef-a6c.healthcare_dataset.analyse_dataset"
GCS_TEMP_BUCKET = f"gs://{BUCKET_NAME}/temp/"
GCS_ABNORMAL_BUCKET = f"gs://{BUCKET_NAME}/abnormal/"
GCS_INCONCLUSIVE_BUCKET = f"gs://{BUCKET_NAME}/inconclusive/"

# Initialize the SparkSession
spark = SparkSession.builder\
                    .appName("HealthcareDataProcesssing") \
                    .getOrCreate()

# Initialize Google Cloud logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
logger = logging.getLogger("Healthcare-data-pipeline")

# Setup Helper Functions
def log_pipeline_step(step, message, level="INFO"):
    if level == "INFO":
        logger.info(f"Step: {step}, Message: {message}")
    elif level == "ERROR":
        logger.error(f"Step: {step}, Message: {message}")
    elif level == "WARNING":
        logger.warning(f"Step: {step}, Message: {message}")
        
# Define schema for CSV data
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Blood Type", StringType(), True),
    StructField("Medical Condition", StringType(), True),
    StructField("Date of Admission", StringType(), True),
    StructField("Doctor", StringType(), True),
    StructField("Hospital", StringType(), True),
    StructField("Insurance Provider", StringType(), True),
    StructField("Billing Amount", DoubleType(), True),
    StructField("Room Number", IntegerType(), True),
    StructField("Admission Type", StringType(), True),
    StructField("Discharge Date", StringType(), True),
    StructField("Medication", StringType(), True),
    StructField("Test Results", StringType(), True),
])

# Functions to validate Data
def validate_data(df):
    log_pipeline_step("Data Validation", "Starting data validation")
    
    validate_df = (df.withColumn("is_normal", when(col("Test Results") == "Normal", True).otherwise(False))\
                    .withColumn("is_abnormal", when(col("Test Results") == "Abnormal", True).otherwise(False))\
                    .withColumn("is_inconclusive", when(col("Test Results") == "Inconclusive", True).otherwise(False))
                )
    
    # Filter records based on flags
    normal_records = validate_df.filter(col("is_normal") == True).drop("is_normal", "is_abnormal", "is_inconclusive")
    alerts_records = validate_df.filter(col("is_abnormal") == True).drop("is_normal", "is_abnormal", "is_inconclusive")
    inconclusive_records = validate_df.filter(col("is_inconclusive") == True).drop("is_normal", "is_abnormal", "is_inconclusive")
                                   
    log_pipeline_step("Data Validation", f"Normal: {normal_records.count()}, Alerts: {alerts_records.count()},\
                      Inconclusive:{inconclusive_records.count()}")
    
    # Returns in order: Normal, Abnormal (Alerts), Inconclusive
    return normal_records, alerts_records, inconclusive_records
 
# Main processing function
def process_data():
    try:
        # Step 1 : Read raw data from GCS
        log_pipeline_step("Data ingestion", "Reading raw data from GCS.")
        df = spark.read.schema(schema).csv(GCS_BUCKET_SOURCE)
        
        # Step 2: Data Validate
        # Unpacking order matches return order: (Normal, Abnormal, Inconclusive)
        normal_df, abnormal_df, inconclusive_df = validate_data(df)
                                
        # Step 3 Log alert records (for auditing)
        if abnormal_df.count() > 0:
            log_pipeline_step("Abnormal Data", "Found Abnormal records.", level="WARNING")
            abnormal_df.write.mode("append").csv(GCS_ABNORMAL_BUCKET)
            
        if inconclusive_df.count() > 0:
            log_pipeline_step("Inconclusive Data", "Found Inconclusive records.", level="WARNING")
            inconclusive_df.write.mode("append").csv(GCS_INCONCLUSIVE_BUCKET)
        
        # Step 4 : Data Transformation (Only on Normal Data)
        df_transformed = normal_df.withColumn("Patient_Key", sha2(col("Name"), 256)) \
                          .drop("Name") \
                          .withColumn("Admission_Date", to_date(col("Date of Admission"), "yyyy-MM-dd")) \
                          .withColumn("Discharge_Date", to_date(col("Discharge Date"), "yyyy-MM-dd")) \
                          .withColumn("Medical_Condition", lower(trim(col("Medical Condition")))) \
                          .withColumn("Billing_Amount", col("Billing Amount").cast(DecimalType(10, 2))) \
                          .withColumn("Length_of_Stay", datediff(col("Discharge_Date"), col("Admission_Date")))

        # Step 5 : Write final record in BigQuery table
        log_pipeline_step("Data Write", "Writing cleaned and transformed data to BigQuery.")
        df_transformed.write\
                .format("bigquery")\
                .option("table", BQ_TABLE)\
                .option("temporaryGCSBucket", GCS_TEMP_BUCKET)\
                .mode("append")\
                .save()
            
    except Exception as e:
        log_pipeline_step("Processing Error", str(e), level="ERROR")
        raise e

# Execute the processing function
if __name__ =="__main__":
    log_pipeline_step("Pipeline Start", "Healthcare data processing pipeline initiated.")
    process_data()
    log_pipeline_step("Pipeline End", "Healthcare data processing pipeline completed.")
