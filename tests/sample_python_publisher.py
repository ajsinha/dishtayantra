import time
from kafka import KafkaProducer

# 1. Initialize the Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: str(v).encode('utf-8')  # Automatically encodes strings to bytes
)

topic_name = 'input_data'

print(f"Loop started. Sending messages to {topic_name}...")

try:
    i = 0
    while i < 10:
        message = '{"message": "hello"}'

        # 2. Send the message
        # .get(timeout=10) makes this synchronous for simpler debugging
        metadata = producer.send(topic_name, value=message).get(timeout=10)

        print(f"Sent: '{message}' to partition {metadata.partition} at offset {metadata.offset}")

        i += 1
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping producer...")

finally:
    # 3. Clean up
    producer.close()