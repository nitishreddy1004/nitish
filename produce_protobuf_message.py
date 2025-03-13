import subprocess
import json
import time
import random
import string
from datetime import datetime
from google.protobuf.json_format import MessageToJson
from sample_pb2 import SampleRecord, Address

def flatten_dict(data, parent_key='', separator='_'):
    items = []
    for key, value in data.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, separator).items())
        else:
            items.append((new_key, value))
    return dict(items)

def produce_protobuf_message(topic, message_count=5):
    for i in range(message_count):
        # Create message data
        name = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
        record = SampleRecord()
        record.id = i
        record.name = name
        record.amount = round(random.uniform(100, 10000), 2)
        record.timestamp = int(datetime.now().timestamp() * 1000)
        record.is_active = random.choice([True, False])
        address = record.address
        address.street = f"{random.randint(100, 999)} Main St"
        address.city = random.choice(["New York", "Los Angeles", "Chicago"])
        address.state = random.choice(["NY", "CA", "IL"])
        address.zip = f"{random.randint(10000, 99999)}"

        # Convert to JSON
        json_str = MessageToJson(record)

        # Flatten the JSON data
        data = json.loads(json_str)
        flattened_data = flatten_dict(data)
        flattened_json = json.dumps(flattened_data)

        # Use the kafka-console-producer tool
        cmd = [
            "sudo", "docker", "exec", "-i", "kafka",
            "bash", "-c",
            f"echo '{flattened_json}' | kafka-console-producer --bootstrap-server kafka:29092 --topic {topic}"
        ]

        print(f"Sending message {i}: {flattened_json}")

        # Execute the command
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            print(f"Error sending message: {stderr.decode()}")
        else:
            print(f"Successfully sent message {i}")

        # Wait between messages
        time.sleep(0.5)

    print(f"Sent {message_count} messages to {topic}")

if __name__ == "__main__":
    produce_protobuf_message("topic1", 10)
