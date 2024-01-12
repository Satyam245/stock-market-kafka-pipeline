import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import json
import time
from pymongo import MongoClient
import hashlib
import boto3

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'YOUR_BOOTSTRAP_SERVERS',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'YOUR_SASL_USERNAME',
    'sasl.password': 'YOUR_SASL_PASSWORD',
    'group.id': 'group1',
    'auto.offset.reset': 'latest'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
    'url': 'YOUR_SCHEMA_REGISTRY_URL',
    'basic.auth.user.info': '{}:{}'.format('YOUR_SCHEMA_REGISTRY_USERNAME', 'YOUR_SCHEMA_REGISTRY_PASSWORD')
})

# Initialize S3 client
s3 = boto3.client('s3', aws_access_key_id='YOUR_AWS_ACCESS_KEY', aws_secret_access_key='YOUR_AWS_SECRET_KEY')

# Fetch the latest Avro schema for the value
subject_name = '<your_topic_name>-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset'],
    'enable.auto.commit': True
})

# Subscribe to the  topic
consumer.subscribe(['your_topic_name'])

# Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0)  # How many seconds to wait for a message

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))

        # Extract the record from the Avro message
        record = msg.value()
        # Generate a unique filename based on some identifier (e.g., timestamp, index)
        filename = f"record_{record['Index']}_{record['Date']}.json"

        # Upload the record as JSON to S3
        s3.put_object(
            Bucket='your_s3_bucket_name',
            Key=filename,
            Body=json.dumps(record),
            ContentType='application/json'
        )

        print(f'Successfully stored record in S3: {filename}')
        time.sleep(1)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
