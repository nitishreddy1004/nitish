import json
from kafka import KafkaProducer
from sample_pb2 import SampleRecord

def flatten_dict(data, parent_key='', separator='_'):
    """Recursively flatten a nested dictionary."""
    items = []
    for key, value in data.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, separator).items())
        else:
            items.append((new_key, value))
    return dict(items)

def produce_flattened_json(topic):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Create a Protobuf message
    record = SampleRecord()
    record.id = 1
    record.name = "John Doe"
    record.amount = 100.0
    record.timestamp = 1643723400
    record.is_active = True
    address = record.address
    address.street = "123 Main St"
    address.city = "New York"
    address.state = "NY"
    address.zip = "10001"

    # Convert Protobuf to JSON and flatten it
    from google.protobuf.json_format import MessageToDict
    json_data = MessageToDict(record)
    flattened_data = flatten_dict(json_data)

    # Send flattened JSON to Kafka
    producer.send(topic, value=flattened_data)
    producer.flush()
    print(f"Sent message: {flattened_data}")

if __name__ == "__main__":
    produce_flattened_json("topic1")
