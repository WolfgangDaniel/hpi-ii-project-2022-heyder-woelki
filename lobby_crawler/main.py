import logging
import os

import click

from lobby_crawler.lobby_extractor import LobbyExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


# @click.command()

def run():
    LobbyExtractor().extract()


if __name__ == "__main__":
    run()
