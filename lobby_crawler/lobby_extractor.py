import logging
from time import sleep

import requests
from parsel import Selector

from build.gen.bakdata.lobby.v1.lobby_pb2 import Lobby
from lobby_producer import LobbyProducer

log = logging.getLogger(__name__)


class LobbyExtractor:
    def __init__(self):
        self.producer = LobbyProducer()

    def extract(self):
        lobbydatafile = LobbyExtractor.send_request()
        # nehme Json Datei, in results und dort über Liste iterieren
        # -> in lobby.proto passendes Schema erstellen (überlegen welche Daten wir wollen)
        # aus jedem Objekt in der Liste die entsprechenden Attribute mit dem proto Schema mappen
        for (lobbyEntry in lobbydatafile["results"]):
            try:
                lobby = Lobby()
                lobby.id = lobbyEntry.registerNumber
                # more

                self.producer.produce_to_topic(lobby=lobby)
                log.debug(lobby)
            except Exception as ex:
                log.error(f"Error: {ex}")
                continue
        exit(0)
        
    @staticmethod
    def send_request() -> str:
        url = f"https://www.lobbyregister.bundestag.de/sucheDetailJson?sort=REGISTRATION_DESC"
        return requests.get(url=url)
