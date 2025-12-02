import os
import random
import time
from datetime import datetime
from json import dumps

from faker import Faker
from kafka import KafkaProducer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "news_stream")
INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda payload: dumps(payload).encode("utf-8"),
        linger_ms=5,
        retries=5,
    )


NEWS_SOURCES = [
    "Reuters",
    "Bloomberg",
    "Al Jazeera",
    "BBC",
    "Guardian",
    "CNN",
    "AP News",
]


def random_news(faker: Faker) -> dict:
    source = random.choice(NEWS_SOURCES)
    now = datetime.utcnow()

    # Faker generate text, sentences, custom paragraphs, etc.
    headline = faker.sentence(nb_words=random.randint(8, 14))
    paragraph = faker.paragraph(nb_sentences=random.randint(2, 4))
    combined_news = f"{headline} {paragraph}"

    return {
        "source": source,
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "news": combined_news,
    }


def produce_news():
    faker = Faker()
    producer = build_producer()

    while True:
        news_event = random_news(faker)
        producer.send(topic=TOPIC, value=news_event)
        print(f"Produced news: {news_event}", flush=True)
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    while True:
        try:
            produce_news()
        except Exception as exc:
            print(f"Producer error: {exc}, retrying in 5s", flush=True)
            time.sleep(5)
