# Use the official Airflow image as a base
FROM apache/airflow:2.9.3 

# Install necessary Python packages
RUN pip install --no-cache-dir pyhive thrift

