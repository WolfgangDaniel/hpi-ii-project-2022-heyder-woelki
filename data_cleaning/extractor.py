import logging
import pandas as pd 
import numpy as np
import recordlinkage
import pandas_dedupe
from pyjarowinkler import distance
from tabulate import tabulate

from consumer.personConsumer import PersonConsumer
from build.gen.bakdata.person.v1.person_pb2 import Person

from clean_person_producer import CleanPersonProducer



log = logging.getLogger(__name__)

class Extractor:
    def __init__(self):
        self.person_producer = CleanPersonProducer()
        

    def extract(self):

        cons = PersonConsumer()
        msgs = cons.consume()
        #lobbyPerson = LobbyPerson()
        msg_lobby_person = msgs["lobby-person"]
        msg_corporate_person = msgs["corporate-person"]
        # corporate_person_df = pd.DataFrame(msg_corporate_person)
        # corporate_person_df['lobbyCompanyName'] = ""
        # msg_lobby_person = msgs["lobby-person"]
        # lobby_person_df =  pd.DataFrame(msg_lobby_person)
        # lobby_person_df['city'] = ""
        # lobby_person_df['birthdate'] = ""
        # lobby_person_df['corporateName'] = ""
        # lobby_person_df['corporateID'] = ""
        # person_df = pd.concat([corporate_person_df,lobby_person_df],ignore_index=True)
        # print(person_df.head())
        # person_df_dedupe = pandas_dedupe.dedupe_dataframe(person_df, person_df.columns, canonicalize=True, sample_size=0.1)
        # person_df_dedupe.to_csv('person_df_dedupe.csv')

        def produce(entrance,matches,person_id):
            person_produce = Person()
            print(person_produce)
            person_produce.id = person_id
            person_produce.firstname = entrance['firstname']
            person_produce.lastname = entrance['lastname']
            
            corporateName = []
            corporateID = []
            lobbyCompanyName=[]
            city = ""
            birthdate = ""
            matches.append(entrance)
            for k in matches:
                if k['corporateName'] != "":
                    corporateName.append(k['corporateName'])
                if k['corporateID'] != "":
                    corporateID.append(k['corporateID'])
                if k['lobbyCompanyName'] != "":
                    lobbyCompanyName.append(k['lobbyCompanyName'])
                if k['birthdate'] != "":
                    birthdate = k['birthdate']
                if k['city'] != "":
                    city = k['city']
            person_produce.city = city
            person_produce.birthdate = birthdate
            person_produce.corporateName.extend(corporateName)
            person_produce.corporateID.extend(corporateID)
            person_produce.lobbyCompanyName.extend(lobbyCompanyName)
            self.person_producer.produce_to_topic(person=person_produce)


        personList=[]
        for person in msg_lobby_person:
            person_dic = {}
            person_dic['firstname'] = person.firstname
            lastname = person.lastname
            person_dic['lastname'] = lastname.replace(" ", "")
            person_dic['city'] = ""
            person_dic['birthdate'] = ""
            person_dic['corporateName'] = ""
            person_dic['corporateID'] = ""
            person_dic['lobbyCompanyName'] = person.lobbyCompanyName
            personList.append(person_dic)
        for person in msg_corporate_person:
            person_dic = {}
            #print(person)
            person_dic['firstname'] = person.firstname
            lastname = person.lastname
            person_dic['lastname'] = lastname.replace(" ", "")
            person_dic['city'] = person.city
            person_dic['birthdate'] = person.birthdate
            person_dic['corporateName'] = person.corporateName
            person_dic['corporateID'] = person.corporateID
            person_dic['lobbyCompanyName'] = ""
            personList.append(person_dic)
        
        person_df_pre_shuffeled = pd.DataFrame(personList)
        # print(tabulate(person_df_pre_sorted.tail(), headers="keys", tablefmt="psql"))
        person_df_pre_sorted = person_df_pre_shuffeled.sample(frac=1).reset_index(drop=True)
        person_df = person_df_pre_sorted.sort_values(by='lastname').reset_index(drop=True)
        
        print(tabulate(person_df.tail(100), headers="keys", tablefmt="psql"))
        duplicate_counter = 0
        lobby_coperate_match = 0
        already_added = []
        person_id = 0
        for i in reversed(range(len(person_df))):
            if (i in already_added):
                continue
            person = person_df.loc[i]
            if person['lobbyCompanyName'] != "":
                print('LOBBY YEAH')
            #print("Person: ", person)
            matches = []
            for j in range(i-100,i):
                if (j in already_added):
                    continue
                compare = person_df.loc[j]
                #print("Compared Person: ", compare)
                try:
                    firstname_distance = distance.get_jaro_distance(person['firstname'],compare['firstname'],winkler=True,scaling=0.1)
                    lastname_distance = distance.get_jaro_distance(person['lastname'],compare['lastname'],winkler=True,scaling=0.1)
                except:
                    continue
                if((firstname_distance + lastname_distance)/2 > 0.9):
                    if(person['city'] == "" or compare['city'] == ""):
                        if(person['birthdate'] == "" or compare['birthdate'] == ""):
                            duplicate_counter +=1
                            matches.append(compare)
                            already_added.append(j)
                            if((person['lobbyCompanyName']==""and compare['lobbyCompanyName']!="") or (person['lobbyCompanyName']!=""and compare['lobbyCompanyName']=="")):
                                print("found lobby company match:")
                                print(person)
                                print(compare)
                                lobby_coperate_match +=1
                        else:
                            if(person['birthdate'] == compare['birthdate']):
                                duplicate_counter +=1
                                matches.append(compare)
                                already_added.append(j)
                    else:
                        city_distance = distance.get_jaro_distance(person['city'],compare['city'],winkler=True,scaling=0.1)
                        if(city_distance>0.9):
                            if(person['birthdate'] == "" or compare['birthdate'] == ""):
                                duplicate_counter +=1
                                matches.append(compare)
                                already_added.append(j)
                            else:
                                if(person['birthdate'] == compare['birthdate']):
                                    duplicate_counter +=1
                                    matches.append(compare)
                                    already_added.append(j)
            produce(person,matches,person_id)
            matches = []
            person_id +=1
        print('matches: ',duplicate_counter)
        print('lobby corperate matches: ',duplicate_counter)
            



        # person_df_dedupe = pandas_dedupe.dedupe_dataframe(person_df, ['firstname','lastname','city','birthdate'], canonicalize=False, sample_size=0.01)
        # person_df_dedupe.to_csv('person_df_dedupe.csv')

        # corporate_id = 0
        # for corporate_event in msg_corporate_events:
        #     try:
        #         corporatePerson.id = corporate_id
        #         parser_res = parse(corporate_event.information, corporate_event.state)
        #         corporatePerson.firstname = parser_res["person_first_name"]
        #         corporatePerson.lastname = parser_res["person_last_name"]
        #         corporatePerson.city = parser_res["person_place_of_birth"]
        #         corporatePerson.birthdate = parser_res["person_birthdate"]
        #         corporatePerson.corporateName = corporate_event.company_name
        #         corporatePerson.corporateID = corporate_event.id
        #         self.corporate_person_producer.produce_to_topic(corporatePerson=corporatePerson)
        #         corporate_id += 1
        #     except:
        #         continue

        '''lobby_id = 0
        for lobby_event in msg_lobby_events:
            for name in lobby_event.lobbyEmployyeNames:
                lobbyPerson.id = lobby_id
                lobbyPerson.firstname = name[name.find(",")+2:]
                lobbyPerson.lastname = name[:name.find(",")] 
                lobbyPerson.lobbyCompanyName = lobby_event.companyName
            self.lobby_person_producer.produce_to_topic(lobbyPerson=lobbyPerson)
            lobby_id += 1'''