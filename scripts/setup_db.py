#!/usr/bin/env python3
"""Create the news database and all tables in ClickHouse."""

import logging
from newschat.db import init_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def main():
    logging.info("Creating news database and tables...")
    init_schema()
    logging.info("Done.")


if __name__ == "__main__":
    main()
