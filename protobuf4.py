from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from sample_pb2 import SampleRecord, Address  # ✅ Ensure Address is imported
from key_pb2 import KeyRecord  # ✅ Add a Protobuf key schema

# Schema Registry Configuration
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Protobuf Serializers
key_serializer = ProtobufSerializer(
    KeyRecord,
    schema_registry_client,
    {'use.deprecated.format': False}  # ✅ Fixes compatibility
)

value_serializer = ProtobufSerializer(
    SampleRecord,
    schema_registry_client,
    {'use.deprecated.format': False}  # ✅ Ensures compatibility
)

# Kafka Producer Configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': key_serializer,  # ✅ Fixed key serializer
    'value.serializer': value_serializer
}

producer = SerializingProducer(producer_conf)

def produce_protobuf_message(topic, message_count=5):
    """Produce Protobuf messages to Kafka."""
    for i in range(message_count):
        # ✅ Properly structured KeyRecord
        key = KeyRecord(key=f"key_{i}")

        # ✅ Correct SampleRecord usage
        record = SampleRecord(
            id=i,
            name=f"User_{i}",
            amount=round(100 + i * 10.5, 2),
            timestamp=1643723400 + i * 1000,
            is_active=(i % 2 == 0),
            address=Address(
                street=f"{100 + i} Main St",
                city="New York",
                state="NY",
                zip=f"{10000 + i}"
            )
        )

        producer.produce(topic=topic, key=key, value=record)  # ✅ Fixed key usage
        producer.poll(0)  # ✅ Ensures message delivery without blocking

        print(f"Sent message {i}: {record}")

    producer.flush()
    print(f"All {message_count} messages sent to topic '{topic}'.")

if __name__ == "__main__":
    produce_protobuf_message("topic1", 10)
