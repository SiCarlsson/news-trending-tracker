import logging

import json
from kafka import KafkaProducer
from itemadapter import ItemAdapter

logger = logging.getLogger(__name__)


class KafkaPipeline:
    def __init__(self):
        self.producer = None

    def open_spider(self, spider):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=["localhost:9092"],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                    "utf-8"
                ),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                compression_type="gzip",
                retries=3,
            )
            logger.info("Kafka producer initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")

    def close_spider(self, spider):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer flushed and closed.")

    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        item_type = type(item).__name__.lower()

        topic_mapping = {
            "websiteitem": "news-websites",
            "articleitem": "news-articles",
            "worditem": "news-words",
            "occurrenceitem": "news-occurrences",
        }

        topic = topic_mapping.get(item_type, f"news-{item_type}")

        try:
            self.producer.send(topic, item_dict)
            logger.info(f"Sent item to topic {topic}: {item_dict}")
        except Exception as e:
            logger.error(f"Failed to send item to topic {topic}: {e}")

        return item
