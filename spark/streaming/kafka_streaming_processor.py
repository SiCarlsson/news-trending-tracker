"""KafkaStreamingProcessor class for orchestrating the streaming pipeline.

Module contains the main processor class that manages the complete lifecycle
of the Kafka to BigQuery streaming pipeline through Spark.
"""

import logging
from models.kafka_schemas import topic_config
from core.spark_factory import SparkSessionFactory
from streaming.kafka_streaming_service import KafkaStreamingService


class KafkaStreamingProcessor:
    """
    Main processor for news streaming application from Kafka.

    Manages the complete lifecycle of the Kafka to BigQuery streaming pipeline,
    including Spark session management and streaming query orchestration.
    """

    def __init__(self):
        """Initialize the KafkaStreamingProcessor with empty state."""
        self.logger = logging.getLogger(__name__)
        self.spark = None
        self.kafka_streaming_service = None
        self.queries = []

    def start(self):
        """
        Start the streaming application.

        Creates Spark session, initializes streaming service, sets up all
        topic processors, and begins streaming data from Kafka to BigQuery.

        Raises:
            Exception: If there's an error during startup.
        """
        try:
            self.spark = SparkSessionFactory.create_session()
            self.kafka_streaming_service = KafkaStreamingService(self.spark)

            self.logger.info(
                "Starting Kafka -> Spark -> BigQuery streaming for all topics..."
            )

            for topic, config in topic_config.items():
                # Use aggregation for occurrences topic, regular processing for others
                if topic == "news-occurrences":
                    queries = self.kafka_streaming_service.process_topic_with_aggregation(
                        topic, config
                    )
                    self.queries.extend(queries)
                else:
                    query = self.kafka_streaming_service.process_topic(topic, config)
                    self.queries.append(query)

            self.logger.info(
                f"All {len(self.queries)} streaming queries started successfully!"
            )
            self.logger.info(f"Topics being processed: {list(topic_config.keys())}")

            self._await_termination()

        except Exception as e:
            self.logger.error(f"Error starting streaming application: {e}")
            self.stop()
            raise

    def _await_termination(self):
        """
        Wait for all streaming queries to terminate.

        Blocks until all streaming queries complete. Signal handlers
        will manage graceful shutdown.
        """
        for query in self.queries:
            query.awaitTermination()

    def stop(self):
        """
        Stop all streaming queries and Spark session.

        Gracefully shuts down all active streaming queries and the Spark session.
        Ensures proper cleanup of resources.
        """

        self.logger.info("Stopping streaming queries...")

        for query in self.queries:
            if query.isActive:
                query.stop()

        if self.spark:
            self.spark.stop()

        self.logger.info("All streaming queries stopped.")
