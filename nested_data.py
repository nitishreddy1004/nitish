#!/usr/bin/env python3
import subprocess
import json
import time
import random
import string
from datetime import datetime

def generate_random_cities():
    """Generate a list of random cities"""
    cities = []
    city_names = ["New York", "Los Angeles", "Chicago", "Houston", "Miami"]
    
    for i in range(random.randint(1, 3)):  # 1-3 cities per state
        cities.append({
            "city_id": random.randint(100, 999),
            "city_name": random.choice(city_names)
        })
    
    return cities

def generate_random_addresses():
    """Generate a list of random addresses"""
    addresses = []
    states = [("NY", "New York"), ("CA", "California"), ("IL", "Illinois")]

    for _ in range(random.randint(1, 2)):  # 1-2 addresses per user
        state_id, state_name = random.choice(states)
        addresses.append({
            "state_id": random.randint(1, 50),  # Fake state ID
            "state_name": state_name,
            "cities": generate_random_cities(),  # Nested cities
            "street": f"{random.randint(100, 999)} Main St",
            "zip": f"{random.randint(10000, 99999)}"
        })

    return addresses

def produce_protobuf_message(topic, message_count=5):
    """Produce messages using the kafka-protobuf-console-producer tool"""
    for i in range(message_count):
        # Create message data
        name = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
        message = {
            "id": i,
            "name": name,
            "amounts": [round(random.uniform(10, 100), 2) for _ in range(3)],  # 3 random amounts
            "timestamp_ntz": int(datetime.now().timestamp() * 1000),  # Milliseconds timestamp
            "is_active": random.choice([True, False]),
            "addresses": generate_random_addresses()
        }

        # Convert to JSON
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
            }
            repeated City cities = 3;
            string street = 4;
            string zip = 5;
          }
          repeated Address addresses = 6;
        }
        """

        # The command has to be carefully constructed to avoid issues with quotes
        cmd = [
            "sudo", "docker", "exec", "-i", "schema-registry",
            "bash", "-c",
            f"echo '{json_str}' | kafka-protobuf-console-producer " +
            f"--bootstrap-server kafka:29092 " +
            f"--topic {topic} " +
            f"--property schema.registry.url=http://schema-registry:8081 " +
            f"--property value.schema='{schema_definition}'"
        ]

        print(f"Sending message {i}: {json_str}")

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
