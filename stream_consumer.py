"""Compatibility entry point for the Wikimedia Kafka-to-SQLite consumer."""
import logging
import os

from wikimedia_stream_consumer import main


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    main()
