import logging

from corporate_person_producer import CorporatePersonProducer
from lobby_person_producer import LobbyPersonProducer
from consumer.lobbyCorporateConsumer import LobbyCorporateConsumer

from build.gen.bakdata.person.v1.corporatePerson_pb2 import CorporatePerson
from build.gen.bakdata.person.v1.lobbyPerson_pb2 import LobbyPerson


log = logging.getLogger(__name__)

class Extractor:
    def __init__(self):
        self.corporate_person_producer = CorporatePersonProducer()
        self.lobby_person_producer = LobbyPersonProducer()
        

    def extract(self):
        try:
            cons = LobbyCorporateConsumer()
            msgs = cons.consume()
            corporatePerson = CorporatePerson()
            lobbyPerson = LobbyPerson()

            msg_corporate_events = msgs["corporate-events"]
            msg_lobby_events = msgs["lobby-events"]

            corporate_id = 0
            for corporate_event in msg_corporate_events:
                corporatePerson.id = corporate_id
                corporatePerson.firstname = corporate_event.person_first_name
                corporatePerson.lastname = corporate_event.person_last_name
                corporatePerson.city = corporate_event.address_city
                corporatePerson.birthdate = corporate_event.person_birthday
                corporatePerson.corporateName = corporate_event.company_name
                corporatePerson.corporateID = corporate_event.id
                self.corporate_person_producer.produce_to_topic(corporatePerson=corporatePerson)
                corporate_id += 1

            lobby_id = 0
            for lobby_event in msg_lobby_events:
                for name in lobby_event.lobbyEmployyeNames:
                    lobbyPerson.id = lobby_id
                    lobbyPerson.firstname = name # ToDo: parse firstname
                    lobbyPerson.lastname = name # ToDo: parse lastname
                self.lobby_person_producer.produce_to_topic(lobbyPerson=lobbyPerson)
                lobby_id += 1
        except Exception as ex:
            log.error(f"Cause: {ex}")