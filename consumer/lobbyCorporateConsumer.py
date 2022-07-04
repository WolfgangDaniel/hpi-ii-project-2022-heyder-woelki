import logging

from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate
from build.gen.bakdata.lobby.v1.lobby_pb2 import Lobby

from consumer.consumer import consume_topic

log = logging.getLogger(__name__)

class LobbyCorporateConsumer:

    def consume(self):
        msg_corporate_events = consume_topic('corporate-events', Corporate, 'new-consumer4')
        log.info("Corporate events consumed.")
        print(msg_corporate_events[0])
        msg_lobby_events = consume_topic('lobby-events', Lobby, 'new-consumer4')
        log.info("Lobby events consumed.")
        print(msg_lobby_events[0])

        return {
            "corporate-events": msg_corporate_events,
            "lobby-events": msg_lobby_events
        }