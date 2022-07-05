import logging

from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate
from build.gen.bakdata.lobby.v1.lobby_pb2 import Lobby

from consumer.consumer import consume_topic

log = logging.getLogger(__name__)

class PersonConsumer:

    def consume(self):
        msg_corporate_person = consume_topic('corporate-person', Corporate, 'person-consumer1')
        log.info("Corporate persons consumed.")
        print(msg_corporate_person[0])
        msg_lobby_person = consume_topic('lobby-person', Lobby, 'person-consumer1')
        log.info("Lobby persons consumed.")
        print(msg_lobby_person[0])

        return {
            "corporate-person": msg_corporate_person,
            "lobby-person": msg_lobby_person
        }