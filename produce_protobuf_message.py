import json
import time
import random
import string
from datetime import datetime
from kafka import KafkaProducer
from google.protobuf.json_format import MessageToJson
from sample_pb2 import SampleRecord

def produce_protobuf_as_json(topic, message_count=5):
    # Initialize Kafka producer with JSON serializer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for i in range(message_count):
        # Create a Protobuf message
        record = SampleRecord()
        record.id = i
        record.name = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
        record.amount = round(random.uniform(100, 10000), 2)
        record.timestamp = int(datetime.now().timestamp() * 1000)
        record.is_active = random.choice([True, False])
        address = record.address
        address.street = f"{random.randint(100, 999)} Main St"
        address.city = random.choice(["New York", "Los Angeles", "Chicago"])
        address.state = random.choice(["NY", "CA", "IL"])
        address.zip = f"{random.randint(10000, 99999)}"

        # Serialize Protobuf message to JSON
        json_message = MessageToJson(record)

        # Send JSON message to Kafka
        producer.send(topic, value=json.loads(json_message))
        print(f"Sent message {i}: {json_message}")

        # Wait between messages
        time.sleep(0.5)

    producer.flush()
    print(f"Sent {message_count} messages to topic '{topic}'.")

if __name__ == "__main__":
    produce_protobuf_as_json("topic1", 10)
