# Stock Market Kafka Project 🚀📈

## Overview

Welcome to my small project, the "Stock Market Kafka Project"! In this project, I've crafted a robust data pipeline using Python, Confluent Kafka, AWS S3, IAM, Glue, and Athena. The goal is to efficiently process and analyze stock market data through a well-defined workflow.

## Technologies Used

- **Programming Language:** Python

- **Amazon Web Services (AWS):**
  - S3 (Simple Storage Service)
  - Athena
  - AWS Glue
    - Glue Crawler
    - Glue Catalog
  - Amazon EC2

- **Confluent Kafka**


## Workflow Overview 📊

1. **Read data from CSV:**
   - Utilize Python to read stock market data from a CSV file, preparing it for further processing.

2. **Send it to Confluent Kafka with Avro schema:**
   - Leverage Confluent Kafka to efficiently stream the data with Avro schema for effective serialization.

3. **Consume, transform, and store individually in AWS S3 as JSON:**
   - Develop a consumer to process and transform the data, storing it individually in AWS S3 in JSON format.

4. **Glue Crawler does its magic, creating a data catalog:**
   - Employ AWS Glue Crawler to automatically discover and catalog the data in the S3 bucket, enhancing data management.

5. **Athena steps in, empowering me to query insights seamlessly:**
   - Utilize AWS Athena for effortless querying and gaining valuable insights from the processed data.
