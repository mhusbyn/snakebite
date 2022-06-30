from datetime import timedelta

import requests
from cloudevents.http import CloudEvent, to_structured
from requests import Response

from config import PULSAR_URL, TENANT, NAMESPACE, TOPIC_NAME


def create_event() -> CloudEvent:
    attributes = {
        "type": "com.example.sampletype1",
        "source": "https://example.com/event-producer",
    }
    data = {"message": "Hello World!"}
    return CloudEvent(attributes, data)


def send_event(event: CloudEvent) -> Response:
    headers, body = to_structured(event)
    headers['PulsarUrl'] = PULSAR_URL
    return requests.post(f'http://localhost:8085/v2/firehose/p/{TENANT}/{NAMESPACE}/{TOPIC_NAME}', data=body, headers=headers, timeout=timedelta(seconds=2).total_seconds())


if __name__ == '__main__':
    event = create_event()
    resp = send_event(event)
    print(resp.status_code)
    print(resp.text)
    print(resp.raw)
