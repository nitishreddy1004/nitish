from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer  # ✅ Use StringSerializer
from sample_pb2 import SampleRecord, Address  # ✅ Import Address

# Configure Schema Registry client
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# ✅ Add required format compatibility setting
protobuf_serializer = ProtobufSerializer(
    SampleRecord,
    schema_registry_client,
    {"use.deprecated.format": False}  # 🚀 Explicitly disable deprecated format
)

# ✅ Fix `key.serializer` by using `StringSerializer()`
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf-8'),  # ✅ Fix key serialization
    'value.serializer': protobuf_serializer
}
producer = SerializingProducer(producer_conf)

def produce_protobuf_message(topic, message_count=5):
    """Produce Protobuf messages to Kafka."""
    for i in range(message_count):
        # ✅ Correctly reference Address inside SampleRecord
        record = SampleRecord(
            id=i,
            name=f"User_{i}",
            amount=round(100 + i * 10.5, 2),
            timestamp=1643723400 + i * 1000,
            is_active=(i % 2 == 0),
            address=Address(  # ✅ Correct Address reference
                street=f"{100 + i} Main St",
                city="New York",
                state="NY",
                zip=f"{10000 + i}"
            )
        )

        # ✅ Ensure correct Key serialization (String)
        producer.produce(
            topic=topic,
            key=str(i),  # ✅ Convert key to string
            value=record,
            on_delivery=delivery_report  # ✅ Keep on_delivery
        )
        print(f"✅ Sent message {i}: {record}")

    # Wait for all messages to be sent
    producer.flush()
    print(f"✅ All {message_count} messages sent to topic '{topic}'.")

def delivery_report(err, msg):
    """Callback function to confirm message delivery."""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

if __name__ == "__main__":
    produce_protobuf_message("topic1", 10)
