import logging
import os

import click

from rb_crawler.constant import State
from rb_crawler.rb_extractor import RbExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


def run():
    RbExtractor().extract()


if __name__ == "__main__":
    run()
