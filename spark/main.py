"""Main entry point for the Spark streaming application.

This module serves as the application entry point, setting up logging
and starting the news streaming processor.
"""

from config import Config
from spark.streaming.kafka_streaming_processor import KafkaStreamingProcessor


def main():
    """
    Main function to start the news streaming application.

    Sets up logging configuration and starts the KafkaStreamingProcessor
    to begin streaming data from Kafka to BigQuery through Spark.
    """
    Config.setup_logging()

    processor = KafkaStreamingProcessor()
    processor.start()


if __name__ == "__main__":
    main()
