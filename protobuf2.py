import time
import random
import string
import json
from datetime import datetime
from confluent_kafka import Producer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from sample_pb2 import SampleRecord, Address  # Import generated Protobuf class

# Kafka & Schema Registry Configuration
KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "topic1"

# Schema Registry Client
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# Fetch latest Protobuf schema
schema_str = schema_registry_client.get_latest_version(f"{TOPIC}-value").schema.schema_str
protobuf_serializer = ProtobufSerializer(SampleRecord, schema_registry_client)

# Kafka Producer Configuration
producer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "key.serializer": str.encode,
    "value.serializer": protobuf_serializer
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    """Delivery report callback triggered upon message delivery"""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def generate_sample_record():
    """Generate a random sample record following the Protobuf schema"""
    return SampleRecord(
        id=random.randint(1, 10000),
        name="".join(random.choices(string.ascii_uppercase, k=10)),
        amount=round(random.uniform(100, 10000), 2),
        timestamp=int(datetime.now().timestamp() * 1000),
        is_active=random.choice([True, False]),
        address=Address(
            street=f"{random.randint(100, 999)} Main St",
            city=random.choice(["New York", "Los Angeles", "Chicago"]),
            state=random.choice(["NY", "CA", "IL"]),
            zip=f"{random.randint(10000, 99999)}"
        )
    )

# Produce 10 messages
for _ in range(10):
    record = generate_sample_record()
    producer.produce(
        TOPIC,
        key=str(record.id),
        value=record,
        on_delivery=delivery_report,
        context=SerializationContext(TOPIC, MessageField.VALUE)
    )
    time.sleep(0.5)

# Wait for outstanding messages
producer.flush()
