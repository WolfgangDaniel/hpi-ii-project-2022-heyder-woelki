import logging
from time import sleep

import requests
from parsel import Selector

from build.gen.bakdata.lobby.v1.lobby_pb2 import Lobby
from lobby_producer import LobbyProducer
import json 

log = logging.getLogger(__name__)


class LobbyExtractor:
    def __init__(self):
        self.producer = LobbyProducer()
        self.url = f"https://www.lobbyregister.bundestag.de/sucheDetailJson?sort=REGISTRATION_DESC"

    def extract(self):
        lobbydatafile = requests.get(url=self.url).json()
        results = lobbydatafile["results"]
        # nehme Json Datei, in results und dort über Liste iterieren
        # -> in lobby.proto passendes Schema erstellen (überlegen welche Daten wir wollen)
        # aus jedem Objekt in der Liste die entsprechenden Attribute mit dem proto Schema mappen
        for lobbyEntry in results:
            # try:
            lobbyEntryDetails = lobbyEntry["registerEntryDetail"] 
            print(lobbyEntry)
            if lobbyEntryDetails["lobbyistIdentity"]["identity"]  == "ORGANIZATION":
                lobby = Lobby()
                lobby.id = lobbyEntry["registerNumber"]
                
               
                lobby.companyName = lobbyEntryDetails["lobbyistIdentity"]["name"]
                lobby.lobbyEmployeeCountFrom = lobbyEntryDetails["employeeCount"]["from"]
                lobby.lobbyEmployeeCountTo = lobbyEntryDetails["employeeCount"]["to"]
                employees = lobbyEntryDetails["lobbyistIdentity"]["namedEmployees"]
                employeeNames = []
                for employee in employees:
                    employeeNames.append(employee["lastName"] + ", " + employee["commonFirstName"])
                lobby.lobbyEmployyeNames.extend(employeeNames)
                lobby.reasonForLobbying = lobbyEntryDetails["activityDescription"]
                #ToDO
                lobby.memberships.extend(lobbyEntryDetails["lobbyistIdentity"]["membershipEntries"])
                try:
                    if lobbyEntryDetails["refuseDonationInformation"] or not lobbyEntryDetails["donationInformationRequired"]:
                        log.debug("no donator informaton", lobbyEntryDetails)
                        lobby.donators.extend([])
                    else:
                        donators = lobbyEntryDetails["donators"]
                        donatorsName = []
                        for donator in donators:
                            donatorsName.append(donator["name"])
                        lobby.donators.extend(donatorsName)
                except:
                    log.debug("no donator informaton", lobbyEntryDetails)
                    lobby.donators.extend([])
                try:
                    if lobbyEntryDetails["refuseFinancialExpensesInformation"]:
                        log.debug("no Finance Info")
                        lobby.lobbyMoneySpentFrom = 0
                        lobby.lobbyMoneySpentTo = 0
                        lobby.lobbyMoneySpentYear = ""
                        lobby.lobbyMoneySpentReason = lobbyEntryDetails["refuseFinancialExpensesInformationReason"]
                    else:
                        financialExpenses = lobbyEntryDetails["financialExpensesEuro"]
                        lobby.lobbyMoneySpentFrom = financialExpenses["from"]
                        lobby.lobbyMoneySpentTo = financialExpenses["to"]
                        yearString = financialExpenses["fiscalYearStart"]
                        lobby.lobbyMoneySpentYear = "20"+yearString[yearString.find("/")+1:]
                        lobby.lobbyMoneySpentReason = ""
                except:
                    log.debug("no Finance Info")
                    lobby.lobbyMoneySpentFrom = 0
                    lobby.lobbyMoneySpentTo = 0
                    lobby.lobbyMoneySpentYear = ""
                    lobby.lobbyMoneySpentReason = ""

                # more

                self.producer.produce_to_topic(lobby=lobby)
                log.debug(lobby)
            else:
                log.debug("natural person does not get scraped",lobbyEntryDetails)
            # except Exception as ex:
            #     log.error(f"Error: {ex}")
        exit(0)