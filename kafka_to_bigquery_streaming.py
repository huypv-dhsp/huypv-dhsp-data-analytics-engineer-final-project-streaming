from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    FloatType,

)

KAFKA_BOOTSTRAP_SERVER = "0.0.0.0:29092"
KAFKA_GROUP_ID = "order_items_streaming_to_bigquery"
KAFKA_TOPIC = "data-analytic-engineer.public.order_items"

BIGQUERY_DESTINATION_TABLE = "class-data-analytics-engineer.bigquery_change_data_capture_example.order_items_delta"
GCS_CHECKPOINT_LOCATION = "data-analytics-engineer/streaming_etl/order_items/checkpoint"
TEMPORARY_GCS_BUCKET = "data-analytics-engineer/streaming_etl/order_items/temp"

# Create Spark Session
spark = SparkSession.builder \
    .getOrCreate()
spark.sparkContext.setLogLevel("OFF")

# Setting file stream for hadoop HDFS as GCS GoogleHadoopFileSystem
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')

# Start consume KAFKA to get messages
kafka_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("kafka.group.id", KAFKA_GROUP_ID)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Convert data from binary to string
kafka_stream_df1 = kafka_stream_df.selectExpr("CAST(value AS STRING)")

# Define Message schema
message_schema = StructType(
    [
        StructField("schema", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)

# Select payload data from original message
kafka_stream_df2 = kafka_stream_df1.withColumn("message",
    from_json(col("value"), message_schema)
).selectExpr("CAST(message.payload AS STRING) AS payload_string")


# Define Payload schema
payload_schema = StructType([
     StructField('before', StringType(), True),
     StructField('after', StringType(), True),
     StructField('source', StringType(), True),
     StructField('op', StringType(), True),
     StructField('ts_ms', StringType(), True),
     StructField('transaction', StringType(), True)
     ]
)

# Extract after from payload
kafka_stream_df3 = kafka_stream_df2.withColumn("payload",
    from_json(col("payload_string"), payload_schema)
).selectExpr("CAST(payload.after AS STRING) AS payload_after_string")

# Define after schema
# This schema is based on the table that topic used to export changes
data_schema = StructType(
    [
        StructField('id', IntegerType(), True),
        StructField('order_id', IntegerType(), True),
        StructField('user_id', IntegerType(), True),
        StructField('product_id', IntegerType(), True),
        StructField("inventory_item_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField('created_at', TimestampType(), True),
        StructField('shipped_at', TimestampType(), True),
        StructField('delivered_at', TimestampType(), True),
        StructField("returned_at", TimestampType(), True),
        StructField("sale_price", FloatType(), True),
    ]
)

# Extract all value in payload after
kafka_data_payload_df = kafka_stream_df3.withColumn(
         "after",
         from_json(col("payload_after_string"), data_schema)
     ).select("after.*")

# Write to console
# consoleOutput = (
#     kafka_data_payload_df
#     .writeStream.outputMode("append")
#     .format("console")
#     .option("truncate", "false")
#     .start()
#     .awaitTermination()
# )

# Write to bigquery table BIGQUERY_DESTINATION_TABLE
# The Write process need 2 addtional paramters:
#   + checkpointLocation: to store checkpoint of data
#   + temporaryGcsBucket: to store temporary output
kafka_data_payload_df\
    .writeStream.format("bigquery")\
    .option("table", BIGQUERY_DESTINATION_TABLE) \
    .option("checkpointLocation", GCS_CHECKPOINT_LOCATION) \
    .option("temporaryGcsBucket", TEMPORARY_GCS_BUCKET) \
    .start() \
    .awaitTermination()
