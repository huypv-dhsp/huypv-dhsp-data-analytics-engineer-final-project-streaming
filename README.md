# STREAMING ETL - Setup

## Create and active virtualenv (Python3)
- $ python3 -m virtualenv ./venv
- $ source ./venv/bin/activate
## Install requirement
- Activate venv
- $ pip install -r requirements.txt

# Download jar file
- https://storage.googleapis.com/spark-lib/bigquery/spark-3.1-bigquery-0.28.0-preview.jar
- https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

# Run pyspark job
- export PYSPARK_PYTHON=/path/to/your/python/execute
- spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 --jars ./spark-bigquery-latest_2.12.jar,./gcs-connector-hadoop3-latest.jar ./kafka_to_bigquery_streaming.py

