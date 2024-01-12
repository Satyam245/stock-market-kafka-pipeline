from decimal import Decimal
from time import sleep
from uuid import uuid4, UUID
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd
import math
import time

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': '<your_bootstrap_servers>',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '<your_sasl_username>',
    'sasl.password': '<your_sasl_password>'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': '<your_schema_registry_url>',
  'basic.auth.user.info': '{}:{}'.format('<your_schema_registry_username>', '<your_schema_registry_password>')
})

# Fetch the latest Avro schema for the value
subject_name = '<your_topic_name>-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
})

df=pd.read_csv("indexProcessed.csv")

# Iterate over rows and produce to Kafka
for index, row in df.iterrows():
    record = row.to_dict() 
    producer.produce(topic='your_topic_name', key=str(index), value=record, on_delivery=delivery_report)
    producer.flush()
    time.sleep(1)
