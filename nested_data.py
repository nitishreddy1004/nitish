#!/usr/bin/env python3
import subprocess
import json
import time
import random
import string
from datetime import datetime

def generate_nested_json():
    """Generate deeply nested JSON data with all required data types"""
    name = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))

    # Generate multiple amounts in an array
    amounts = [round(random.uniform(20, 100), 2) for _ in range(3)]

    # Generate timestamp in epoch milliseconds (timestamp_ntz)
    timestamp_ntz = int(datetime.now().timestamp() * 1000)

    # Generate nested address structure
    address = {
        "state_id": random.randint(1, 50),
        "state_name": random.choice(["New York", "California", "Texas", "Florida"]),

        "cities": [
            {
                "city_id": random.randint(100, 999),
                "city_name": random.choice(["Los Angeles", "San Francisco", "Miami", "Houston"]),
                "county": {
                    "county_name": random.choice(["Orange County", "Cook County", "Harris County"]),
                    "zip_code": f"{random.randint(10000, 99999)}"
                }
            },
            {
                "city_id": random.randint(100, 999),
                "city_name": random.choice(["Austin", "Dallas", "Seattle", "Boston"]),
                "county": {
                    "county_name": random.choice(["King County", "Travis County", "Broward County"]),
                    "zip_code": f"{random.randint(10000, 99999)}"
                }
            }
        ]
    }

    # Final message structure
    message = {
        "id": random.randint(1, 999999),
        "name": name,
        "amounts": amounts,
        "timestamp_ntz": timestamp_ntz,
        "is_active": random.choice([True, False]),
        "address": address
    }

    return message

def produce_protobuf_message(topic, message_count=5):
    """Produce messages using the kafka-protobuf-console-producer tool"""
    for i in range(message_count):
        message = generate_nested_json()
        json_str = json.dumps(message)

        # Use the kafka-protobuf-console-producer tool from schema-registry
        schema_definition = """
        syntax = "proto3";
        package com.example;

        message SampleRecord {
          int32 id = 1;
          string name = 2;
          repeated double amounts = 3;
          int64 timestamp_ntz = 4;
          bool is_active = 5;

          message Address {
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

          Address address = 6;
        }
        """

        # Construct Kafka Protobuf producer command
        cmd = [
            "sudo", "docker", "exec", "-i", "schema-registry",
            "bash", "-c",
            f"echo '{json_str}' | kafka-protobuf-console-producer " +
            f"--bootstrap-server kafka:29092 " +
            f"--topic {topic} " +
            f"--property schema.registry.url=http://schema-registry:8081 " +
            f"--property value.schema='{schema_definition}'"
        ]

        print(f"Sending message {i}: {json.dumps(message, indent=2)}")

        # Execute the command
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            print(f"Error sending message: {stderr.decode()}")
        else:
            print(f"Successfully sent message {i}")

        time.sleep(0.5)

    print(f"Sent {message_count} messages to {topic}")

if __name__ == "__main__":
    produce_protobuf_message("topic1", 10)
