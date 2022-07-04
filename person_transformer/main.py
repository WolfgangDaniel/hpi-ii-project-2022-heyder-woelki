import logging
import os

import click

from person_transformer.extractor import Extractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


def run():
    Extractor().extract()


if __name__ == "__main__":
    run()