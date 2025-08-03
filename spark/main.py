"""Main entry point for the Spark streaming application.

Module contains the NewsStreamProcessor class that orchestrates the entire
streaming pipeline from Kafka to BigQuery through Spark.
"""

import signal
import sys
from schemas import topic_config
from spark_factory import SparkSessionFactory
from kafka_streaming_service import KafkaStreamingService


class NewsStreamProcessor:
    """
    Main processor for news streaming application.

    Manages the complete lifecycle of the Kafka to BigQuery streaming pipeline,
    including Spark session management and streaming query orchestration.
    """

    def __init__(self):
        """Initialize the NewsStreamProcessor with empty state."""
        self.spark = None
        self.kafka_streaming_service = None
        self.queries = []
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals gracefully.

        Args:
            signum: Signal number received.
            frame: Current stack frame.
        """
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.stop()
        sys.exit(0)

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

            print("\nStarting Kafka -> Spark -> BigQuery streaming for all topics...")

            for topic, config in topic_config.items():
                query = self.kafka_streaming_service.process_topic(topic, config)
                self.queries.append(query)

            print(f"\nAll {len(self.queries)} streaming queries started successfully!")
            print("Topics being processed:", list(topic_config.keys()))

            self._await_termination()

        except Exception as e:
            print(f"Error starting streaming application: {e}")
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

        print("Stopping streaming queries...")

        for query in self.queries:
            if query.isActive:
                query.stop()

        if self.spark:
            self.spark.stop()

        print("All streaming queries stopped.")


if __name__ == "__main__":
    processor = NewsStreamProcessor()
    processor.start()
