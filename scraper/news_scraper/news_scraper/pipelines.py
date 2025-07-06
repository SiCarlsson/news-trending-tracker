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
        # Determine topic based on item type
        item_dict = ItemAdapter(item).asdict()
        topic = f"news-{type(item).__name__.lower()}"

        self.producer.send(topic, item_dict)
        logger.info(f"Sent item to topic {topic}: {item_dict}")
        return item
    