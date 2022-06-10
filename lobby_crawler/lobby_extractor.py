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
        lobbydatafile = json.load(requests.get(url=self.url))
        results = list(lobbydatafile["results"])
        # nehme Json Datei, in results und dort über Liste iterieren
        # -> in lobby.proto passendes Schema erstellen (überlegen welche Daten wir wollen)
        # aus jedem Objekt in der Liste die entsprechenden Attribute mit dem proto Schema mappen
        for lobbyEntry in results:
            try:
                if lobbyEntry["lobbyistIdentity"]["identity"]  == "ORGANIZATION":
                    lobby = Lobby()
                    lobby.id = lobbyEntry["registerNumber"]
                    
                    lobbyEntryDetails = lobbyEntry["registerEntryDetail"]
                    
                    lobby.companyName = lobbyEntryDetails["lobbyistIdentity"]["name"]
                    lobby.lobbyEmployeeCount.start = lobbyEntryDetails["employeeCount"]["from"]
                    lobby.lobbyEmployeeCount.to = lobbyEntryDetails["employeeCount"]["to"]
                    employees = lobbyEntryDetails["namedEmployees"]
                    for employee in employees:
                        lobby.lobbyEmployyeNames.firstName = employee["commenFirstName"]
                        lobby.lobbyEmployyeNames.lastName = employee["lastName"]
                    lobby.reasonForLobbying = lobbyEntryDetails["activityDescription"]
                    #ToDO
                    lobby.memberships = lobbyEntryDetails["lobbyistIdentity"]["membershipEntries"]
                    if lobbyEntryDetails["refuseDonationInformation"] or not lobbyEntryDetails["donationInformationRequired"]:
                        log.debug("no donator informaton", lobbyEntryDetails)
                        lobby.donators = None
                    else:
                        donators = lobbyEntryDetails["donators"]
                        donatorsName = []
                        for donator in donators:
                            donatorsName.append(donator["name"])
                        if donatorsName != []:
                            lobby.donators = donatorsName
                        else: 
                            lobby.donators = None
                    lobby.intrests
                    if lobbyEntryDetails["refuseFinancialExpensesInformation"]:
                        log.debug("no Finance Info")
                        lobby.LobbyingMonneySpend.start = None
                        lobby.LobbyingMonneySpend.to = None
                        lobby.LobbyingMonneySpend.year = None
                        lobby.LobbyingMonneySpend.reason = lobbyEntryDetails["refuseFinancialExpensesInformationReason"]
                    else:
                        financialExpenses = lobbyEntryDetails["financialExpensesEuro"]
                        lobby.LobbyingMonneySpend.start = financialExpenses["from"]
                        lobby.LobbyingMonneySpend.to = financialExpenses["to"]
                        yearString = financialExpenses["fiscalYearStart"]
                        lobby.LobbyingMonneySpend.year = "20"+yearString[yearString.find("/")+1:]
                        lobby.LobbyingMonneySpend.reason = None

                    # more

                    self.producer.produce_to_topic(lobby=lobby)
                    log.debug(lobby)
                else:
                    log.debug("natural person does not get scraped",lobbyEntryDetails)
            except Exception as ex:
                log.error(f"Error: {ex}")
        exit(0)
