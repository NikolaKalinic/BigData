from confluent_kafka import Producer
import json
from uuid import uuid4


TOPIC = 'html_file'
PRODUCER_CONFIG = {
        "bootstrap.servers": 'kafka:9092',
        'client.id': 'python_producer',
}


def get_uuid():
    return str(uuid4())


def get_valid_data():
    event_id = get_uuid()
    return {
        "id": event_id,
        "category": "TimeTrackingEvent",
        "action": "ReviewTrackerPaging",
        "type": "track",
        "product": "test",
        "app": "test-app",
        "event_ts": "1632748587682",
        "event_sent_ts": "1632748587684",
        "platform": "web",
        "hostname": "test.com",
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "ip_address": "174.68.222.198"
    }


def get_invalid_data():
    return {"no_id": "here"}


def main():
    p = Producer(PRODUCER_CONFIG)
    p.produce(
        topic=TOPIC,
        value=json.dumps(get_valid_data()),
    )
    p.flush()

if __name__ == '__main__':
    main()



