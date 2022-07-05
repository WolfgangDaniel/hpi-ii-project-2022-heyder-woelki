import logging

from build.gen.bakdata.person.v1.corporatePerson_pb2 import CorporatePerson
from build.gen.bakdata.person.v1.lobbyPerson_pb2 import LobbyPerson

from consumer.consumer import consume_topic

log = logging.getLogger(__name__)

class PersonConsumer:

    def consume(self):
        msg_corporate_person = consume_topic('corporate-person', CorporatePerson, 'person-consumer3')
        log.info("Corporate persons consumed.")
        print(msg_corporate_person[0])
        msg_lobby_person = consume_topic('lobby-person', LobbyPerson, 'person-consumer3')
        log.info("Lobby persons consumed.")
        print(msg_lobby_person[0])

        return {
            "corporate-person": msg_corporate_person,
            "lobby-person": msg_lobby_person
        }