from confluent_kafka import Consumer, KafkaError

TOPIC = 'raw'
CONSUMER_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'python_consumer_group',
    'auto.offset.reset': 'earliest',
}

def consume_messages():
    c = Consumer(CONSUMER_CONFIG)
    c.subscribe([TOPIC])

    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            print("KTIA")
            # Print the received message value
            print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == '__main__':
    consume_messages()
