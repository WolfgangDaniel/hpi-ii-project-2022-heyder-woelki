import logging

from clean_person_producer import CleanPersonProducer
from consumer.personConsumer import PersonConsumer

from build.gen.bakdata.person.v1.corporatePerson_pb2 import CorporatePerson
from build.gen.bakdata.person.v1.lobbyPerson_pb2 import LobbyPerson
from rb_crawler.rb_parser import parse


log = logging.getLogger(__name__)

class Extractor:
    def __init__(self):
        self.clean_person_producer = CleanPersonProducer()        

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
                try:
                    corporatePerson.id = corporate_id
                    parser_res = parse(corporate_event.information, corporate_event.state)
                    corporatePerson.firstname = parser_res["person_first_name"]
                    corporatePerson.lastname = parser_res["person_last_name"]
                    corporatePerson.city = parser_res["person_place_of_birth"]
                    corporatePerson.birthdate = parser_res["person_birthdate"]
                    corporatePerson.corporateName = corporate_event.company_name
                    corporatePerson.corporateID = corporate_event.id
                    self.corporate_person_producer.produce_to_topic(corporatePerson=corporatePerson)
                    corporate_id += 1
                except:
                    continue

            lobby_id = 0
            for lobby_event in msg_lobby_events:
                for name in lobby_event.lobbyEmployyeNames:
                    lobbyPerson.id = lobby_id
                    lobbyPerson.firstname = name[name.find(",")+2:] # ToDo: parse firstname
                    lobbyPerson.lastname = name[:name.find(",")] # ToDo: parse lastname
                self.lobby_person_producer.produce_to_topic(lobbyPerson=lobbyPerson)
                lobby_id += 1
        except Exception as ex:
            log.error(f"Cause: {ex}")