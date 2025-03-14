#!/usr/bin/env python3
import subprocess
import json
import time
import random
import string
from datetime import datetime

def generate_nested_data():
    """Generate a deeply nested JSON structure with arrays and large numbers."""
    name = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))

    # Generating large numbers (as strings to prevent precision loss)
    large_number = "154745.67896544345759374856"
    very_large_number = "1436494048564336484950496484"

    # Generate multiple amounts
    amount_list = [round(random.uniform(10, 1000), 2) for _ in range(3)]  # Array of amounts

    message = {
        "id": random.randint(1, 100),
        "name": name,
        "amount": amount_list,  # Ensure amount is an array
        "timestamp_ntz": int(datetime.now().timestamp() * 1000),  # Unix timestamp
        "is_active": random.choice([True, False]),
        "large_number": large_number,  # Wrapped as a string
        "very_large_number": very_large_number,  # Wrapped as a string
        "address": {
            "street": f"{random.randint(100, 999)} Main St",
            "states": [
                {
                    "state_id": random.randint(1, 50),
                    "state_name": random.choice(["New York", "California", "Illinois"]),
                    "cities": [
                        {
                            "city_id": random.randint(1, 1000),
                            "city_name": random.choice(["Los Angeles", "Chicago", "New York"]),
                            "county": {
                                "county_name": random.choice(["Orange County", "Cook County", "Kings County"]),
                                "zip_code": f"{random.randint(10000, 99999)}"
                            }
                        }
                        for _ in range(2)  # Generate 2 cities per state
                    ]
                }
                for _ in range(2)  # Generate 2 states per address
            ]
        }
    }
    return message


def produce_protobuf_message(topic, message_count=5):
    """Produce deeply nested Protobuf messages using kafka-protobuf-console-producer."""
    for i in range(message_count):
        message = generate_nested_data()

        # Convert to JSON safely
        json_str = json.dumps(message, indent=2)

        # Save the JSON message for debugging
        with open(f"message_{i}.json", "w") as f:
            f.write(json_str)

        print(f"Generated JSON for message {i}:")
        print(json_str)

        # Ensure JSON format is valid before sending to Kafka
        try:
            json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON before sending to Kafka: {e}")
            continue  # Skip sending this message if it's invalid

        schema_definition = """
        syntax = "proto3";
        package com.example;
        message SampleRecord {
          int32 id = 1;
          string name = 2;
          repeated double amount = 3;
          int64 timestamp_ntz = 4;
          bool is_active = 5;
          string large_number = 6;
          string very_large_number = 7;
          message Address {
            string street = 1;
            message State {
              int32 state_id = 1;
              string state_name = 2;
              message City {
                int32 city_id = 1;
                string city_name = 2;
                message County {
                  string county_name = 1;
                  string zip_code = 2;
                }
                County county = 3;
              }
              repeated City cities = 3;
            }
            repeated State states = 2;
          }
          Address address = 8;
        }
        """

        cmd = [
            "sudo", "docker", "exec", "-i", "schema-registry",
            "bash", "-c",
            f"echo '{json_str}' | kafka-protobuf-console-producer " +
            f"--bootstrap-server kafka:29092 " +
            f"--topic {topic} " +
            f"--property schema.registry.url=http://schema-registry:8081 " +
            f"--property value.schema='{schema_definition}'"
        ]

        print(f"📤 Sending message {i} to Kafka...")

        # Execute the command
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            print(f"❌ Error sending message {i}: {stderr.decode()}")
        else:
            print(f"✅ Successfully sent message {i}")

        # Wait between messages
        time.sleep(0.5)

    print(f"✅ Sent {message_count} messages to {topic}")

if __name__ == "__main__":
    produce_protobuf_message("topic1", 10)
