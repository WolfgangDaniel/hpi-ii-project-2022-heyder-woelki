import json
import ijson
import logging
from time import sleep

import requests
from parsel import Selector

from build.gen.bakdata.corporate.v1.corporate_pb2 import Corporate, Status
from rb_producer import RbProducer

from rb_parser import parse

log = logging.getLogger(__name__)


class RbExtractor:
    def __init__(self):
        self.producer = RbProducer()

    def extract(self):
        with open('exports\corporate-events-dump.json', 'r') as rb_data:
                line = rb_data.readline()
                while line:
                    try:
                        line_obj = json.loads(line)
                        corporate_event = line_obj['_source']
                        # print(line_obj['_source'])
                        corporate = Corporate()
                        corporate.rb_id = corporate_event['rb_id']
                        corporate.state = corporate_event['state']
                        corporate.reference_id = corporate_event['reference_id']
                        event_type = corporate_event['event_type']
                        corporate.event_date = corporate_event['event_date']
                        corporate.id = corporate_event['id']
                        raw_text: str = corporate_event['information']
                        self.handle_elastic_events(corporate, event_type, raw_text, corporate_event['state'])
                        line = rb_data.readline()
                    except Exception as ex:
                        log.error(f"Cause: {ex}")
                        line = rb_data.readline()
                        continue
        
        # counter = 0
        # with open('exports\corporate-events-dump.json', 'rb') as data:
        #     for corporate_event in ijson.items(data, 'item'):
        #         counter = counter + 1
        #         print(corporate_event)
        #         print("Finished object " + str(counter))

        # while True:
        #     try:
        #         log.info(f"Sending Request for: {self.rb_id} and state: {self.state}")
        #         text = self.send_request()
        #         if "Falsche Parameter" in text:
        #             log.info("The end has reached")
        #             break
        #         selector = Selector(text=text)
        #         corporate = Corporate()
        #         corporate.rb_id = self.rb_id
        #         corporate.state = self.state
        #         corporate.reference_id = self.extract_company_reference_number(selector)
        #         event_type = selector.xpath("/html/body/font/table/tr[3]/td/text()").get()
        #         corporate.event_date = selector.xpath("/html/body/font/table/tr[4]/td/text()").get()
        #         corporate.id = f"{self.state}_{self.rb_id}"
        #         raw_text: str = selector.xpath("/html/body/font/table/tr[6]/td/text()").get()
        #         self.handle_events(corporate, event_type, raw_text)
        #         self.rb_id = self.rb_id + 1
        #         log.debug(corporate)
        #     except Exception as ex:
        #         log.error(f"Skipping {self.rb_id} in state {self.state}")
        #         log.error(f"Cause: {ex}")
        #         self.rb_id = self.rb_id + 1
        #         continue
        # exit(0)

    def send_request(self) -> str:
        url = f"https://www.handelsregisterbekanntmachungen.de/skripte/hrb.php?rb_id={self.rb_id}&land_abk={self.state}"
        # For graceful crawling! Remove this at your own risk!
        sleep(0.001)
        return requests.get(url=url).text

    @staticmethod
    def extract_company_reference_number(selector: Selector) -> str:
        return ((selector.xpath("/html/body/font/table/tr[1]/td/nobr/u/text()").get()).split(": ")[1]).strip()

    def handle_events(self, corporate, event_type, raw_text):
        if event_type == "Neueintragungen":
            self.handle_new_entries(corporate, raw_text, )
        elif event_type == "Veränderungen":
            self.handle_changes(corporate, raw_text)
        elif event_type == "Löschungen":
            self.handle_deletes(corporate)

    def handle_elastic_events(self, corporate, event_type, raw_text, state):
        if event_type == "create":
            self.handle_new_entries(corporate, raw_text, state)
        elif event_type == "delete":
            self.handle_deletes(corporate)

    def handle_new_entries(self, corporate: Corporate, raw_text: str, state: str) -> Corporate:
        log.debug(f"New company found: {corporate.id}")
        corporate.event_type = "create"
        corporate.information = raw_text
        corporate.status = Status.STATUS_ACTIVE
        info = parse(raw_text, state)
        corporate.company_name = info['company_name']
        corporate.address_street = info['street']
        corporate.address_plz = info['plz']
        corporate.address_city = info['city']
        corporate.person_first_name = info['person_first_name']
        corporate.person_last_name = info['person_last_name']
        corporate.person_birthday = info['person_birthdate']
        corporate.person_city = info['person_place_of_birth']
        self.producer.produce_to_topic(corporate=corporate)

    def handle_changes(self, corporate: Corporate, raw_text: str):
        log.debug(f"Changes are made to company: {corporate.id}")
        corporate.event_type = "update"
        corporate.status = Status.STATUS_ACTIVE
        corporate.information = raw_text
        info = parse(raw_text,self.state)
        corporate.company_name = info['company_name']
        corporate.adress_street = info['street']
        corporate.adress_plz = info['plz']
        corporate.adress_city = info['city']
        corporate.person_first_name = info['person_first_name']
        corporate.person_last_name = info['person_last_name']
        corporate.person_birthday = info['person_birthdate']
        corporate.person_city = info['person_place_of_birth']
        self.producer.produce_to_topic(corporate=corporate)

    def handle_deletes(self, corporate: Corporate):
        log.debug(f"Company {corporate.id} is inactive")
        corporate.event_type = "delete"
        corporate.status = Status.STATUS_INACTIVE
        self.producer.produce_to_topic(corporate=corporate)
