"""Start the continuous Wikimedia RecentChange to Kafka producer."""
import logging
import os

from wikimedia_live_ingest import main


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    main()
