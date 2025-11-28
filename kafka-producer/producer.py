import os
import random
import time
from datetime import datetime
from json import dumps

from faker import Faker
from kafka import KafkaProducer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "2"))


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda payload: dumps(payload).encode("utf-8"),
    )


def random_transaction(faker: Faker) -> dict:
    amount = round(random.uniform(100.0, 8000.0), 2)
    card_number = faker.credit_card_number(card_type=None)
    last4 = "".join(filter(str.isdigit, card_number))[-4:]
    return {
        "transaction_id": faker.uuid4(),
        "user_id": faker.uuid4(),
        "merchant": faker.company(),
        "location": faker.city(),
        "card_last4": last4,
        "amount": amount,
        "currency": "USD",
        "timestamp": datetime.utcnow().isoformat(),
        "is_fraudulent": amount > 5000,
    }


def produce_transactions():
    faker = Faker()
    producer = build_producer()

    while True:
        transaction = random_transaction(faker)
        producer.send(topic=TOPIC, value=transaction)
        print(f"Produced transaction: {transaction}", flush=True)
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    while True:
        try:
            produce_transactions()
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Producer error: {exc}, retrying in 5s", flush=True)
            time.sleep(5)