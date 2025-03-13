from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from sample_pb2 import SampleRecord, Address  # ✅ Ensure Address is imported

# Schema Registry Config
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Protobuf Serializer Fix
protobuf_serializer = ProtobufSerializer(
    SampleRecord,
    schema_registry_client,
    {'use.deprecated.format': False}  # ✅ Fixes compatibility
)

# Kafka Producer Config
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': lambda k: k.encode('utf-8'),
    'value.serializer': protobuf_serializer
}

producer = SerializingProducer(producer_conf)

def produce_protobuf_message(topic, message_count=5):
    """Produce Protobuf messages to Kafka."""
    for i in range(message_count):
        # ✅ Ensure Address is correctly used inside SampleRecord
        record = SampleRecord(
            id=i,
            name=f"User_{i}",
            amount=round(100 + i * 10.5, 2),
            timestamp=1643723400 + i * 1000,
            is_active=(i % 2 == 0),
            address=Address(  # ✅ Fixed usage of Address
                street=f"{100 + i} Main St",
                city="New York",
                state="NY",
                zip=f"{10000 + i}"
            )
        )

        producer.produce(topic=topic, key=f"key_{i}", value=record)
        print(f"Sent message {i}: {record}")

    producer.flush()
    print(f"All {message_count} messages sent to topic '{topic}'.")

if __name__ == "__main__":
    produce_protobuf_message("topic1", 10)
