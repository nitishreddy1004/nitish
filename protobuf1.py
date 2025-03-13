from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from sample_pb2 import SampleRecord

# Configure Schema Registry client
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Configure Protobuf serializer
protobuf_serializer = ProtobufSerializer(SampleRecord, schema_registry_client)

# Configure Kafka producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': str.encode,
    'value.serializer': protobuf_serializer
}
producer = SerializingProducer(producer_conf)

def produce_protobuf_message(topic, message_count=5):
    """Produce Protobuf messages to Kafka."""
    for i in range(message_count):
        # Create a Protobuf message
        record = SampleRecord(
            id=i,
            name=f"User_{i}",
            amount=round(100 + i * 10.5, 2),
            timestamp=1643723400 + i * 1000,
            is_active=(i % 2 == 0),
            address=SampleRecord.Address(
                street=f"{100 + i} Main St",
                city="New York",
                state="NY",
                zip=f"{10000 + i}"
            )
        )

        # Send the message to Kafka
        producer.produce(topic=topic, key=f"key_{i}", value=record)
        print(f"Sent message {i}: {record}")

    # Wait for all messages to be sent
    producer.flush()
    print(f"All {message_count} messages sent to topic '{topic}'.")

if __name__ == "__main__":
    produce_protobuf_message("topic1", 10)
