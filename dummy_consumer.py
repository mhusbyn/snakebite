import json
import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pprint import pprint

import requests

from config import PULSAR_URL, TENANT, TOPIC_FULL_NAME


class EchoRequest(BaseHTTPRequestHandler):
    def do_GET(self):
        print('Doing GET')
        logging.info(self.request)
        _, response_body = self.responses[HTTPStatus.OK]
        self.send_response(HTTPStatus.OK, response_body)
        self.end_headers()
        print('finished')

    def do_POST(self):
        print('Doing POST')
        logging.info(self.request)
        _, response_body = self.responses[HTTPStatus.OK]
        self.send_response(HTTPStatus.OK, response_body)
        self.end_headers()
        print('finished')


PORT = 12345


def get_topic():
    headers = {"content-type": "application/json"}
    body = {
        "PulsarURL": PULSAR_URL,
        "TopicFullName": TOPIC_FULL_NAME,
    }
    response = requests.get("http://localhost:8085/v2/topic", data=json.dumps(body), headers=headers)
    print(response.status_code)
    print(response.text)
    pprint(json.loads(response.text))
    response.raise_for_status()

def create_topic():
    headers = {"content-type": "application/json"}
    body = {
        "PulsarURL": PULSAR_URL,
        "Tenant": TENANT,
        "TopicFullName": TOPIC_FULL_NAME,
        "TopicStatus": 1,
    }
    response = requests.post("http://localhost:8085/v2/topic", data=json.dumps(body), headers=headers)
    print(response.status_code)
    print(response.text)
    pprint(json.loads(response.text))
    response.raise_for_status()


def register_webhook():
    create_topic()
    headers = {"content-type": "application/json"}
    body = {
        "PulsarURL": PULSAR_URL,
        "Tenant": TENANT,
        "TopicFullName": TOPIC_FULL_NAME,
        "Webhooks": [{
            "subscription": "test-subscription",
            "subscriptionType": "shared",
            "initialPosition": "earliest",
            "url": f"http://host.docker.internal:{PORT}",
            "webhookStatus": 1
        }],
        "TopicStatus": 1,
    }
    response = requests.post("http://localhost:8085/v2/topic", data=json.dumps(body), headers=headers)
    print(response.status_code)
    print(response.text)
    pprint(json.loads(response.text))
    response.raise_for_status()


if __name__ == "__main__":
    # get_topic()
    register_webhook()

    server_address = ("", PORT)

    with HTTPServer(server_address, EchoRequest) as httpd:
        httpd.serve_forever()
