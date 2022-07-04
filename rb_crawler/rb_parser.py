import re 

def parse(info,state):
    company_name_start = 0
    company_name_end = info.find(",",company_name_start)
    company_name = info[company_name_start:company_name_end]
    company_address_city_start = company_name_end +2
    company_address_city_end = info.find(" ",company_address_city_start)
    company_address_city = info[company_address_city_start:company_address_city_end]
    company_address_street_start = info.find("(", company_address_city_end) +1
    company_address_street_end =company_address_city_end = info.find(",",company_address_street_start)
    company_address_street = info[company_address_street_start:company_address_street_end]
    company_address_plz_start = info.find(",", company_address_street_end) +2
    company_address_plz_end = info.find(" ",company_address_plz_start)
    company_address_plz = info[company_address_plz_start:company_address_plz_end]
    # person = re.findall("(([\w -]+, )([\w -]+, )?([\w./ -]+), \*\d{2}.\d{2}.\d{4})(, )?([\w.\/ -]+)?",info)[0]
    birthday_string = re.findall("\*\d{2}.\d{2}.\d{4}",info)[0]
    person_birthdate = birthday_string[1:]
    birthday_start = info.find(birthday_string)
    if(state == "be"):
        #first_name
        person_first_name_end = info.rfind(",",0,birthday_start)
        person_first_name_start = info.rfind(",",0,person_first_name_end)
        person_first_name = info[person_first_name_start+2:person_first_name_end]
        #last_name
        person_last_name_end = person_first_name_start
        person_last_name_start_sim = info.rfind(";",0,person_last_name_end)-1
        person_last_name_start_com = info.rfind(",",0,person_last_name_end)-1
        person_last_name_start_point = info.rfind(".",0,person_last_name_end)-1
        person_last_name_start = max(person_last_name_start_sim,person_last_name_start_com,person_last_name_start_point)
        person_last_name = info[person_last_name_start+2:person_last_name_end]
        #place_of_birth
        person_place_of_birth_start = info.find(",",birthday_start)
        person_place_of_birth_end = info.find(";",person_place_of_birth_start)
        person_place_of_birth = info[person_place_of_birth_start+2:person_place_of_birth_end]
    else:
        #place_of_birth
        person_place_of_birth_end = info.rfind(",",0,birthday_start)
        person_place_of_birth_start = info.rfind(",",0,person_place_of_birth_end)
        person_place_of_birth = info[person_place_of_birth_start+2:person_place_of_birth_end]
        #first_name
        person_first_name_end = person_place_of_birth_start
        person_first_name_start = info.rfind(",",0,person_first_name_end)
        person_first_name = info[person_first_name_start+2:person_first_name_end]
        #last_name
        person_last_name_end = person_first_name_start
        person_last_name_start = info.rfind(" ",0,person_last_name_end)-1
        person_last_name = info[person_last_name_start+2:person_last_name_end]
    return {"company_name": company_name,
    "city": company_address_city,
    "street":company_address_street,
    "plz": company_address_plz,
    "person_first_name": person_first_name,
    "person_last_name": person_last_name,
    "person_birthdate": person_birthdate,
    "person_place_of_birth":person_place_of_birth}