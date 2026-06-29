from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    retry_backoff_ms=1000,
    request_timeout_ms=30000
)

users = ['alice', 'bob', 'charlie', 'diana']
products = ['laptop', 'mouse', 'keyboard', 'monitor']
actions = ['click', 'purchase', 'view']

print("✅ Producer started! Sending messages to localhost:9092")
print("Press Ctrl+C to stop\n")

message_count = 0

try:
    while True:
        event = {
            'user': random.choice(users),
            'product': random.choice(products),
            'timestamp': time.time(),
            'action': random.choice(actions)
        }

        future = producer.send('user_clicks', value=event)
        record_metadata = future.get(timeout=10)
        message_count += 1

        print(f"[MSG #{message_count}] Sent: {event}")
        print(f"           → Topic: {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")

        time.sleep(1)

except KeyboardInterrupt:
    print(f"\n🛑 Producer stopped. Total messages sent: {message_count}")

except Exception as e:
    print(f"\n❌ Error: {e}")
    print("Is Kafka running? Try: docker-compose up")

finally:
    producer.flush()
    producer.close()
    print("✅ Producer closed cleanly.")