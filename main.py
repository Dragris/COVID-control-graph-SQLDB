from test import App

def clear_db(app):
    app.delete_all()
    # TODO: Clear SQL

def add_country(app, cid, name, population):
    #TODO: ADD TO SQL
    app.create_country(cid, name)

def add_city(app, cid, name, population):
    # TODO: ADD TO SQL
    app.create_city(cid, name)

def add_person(app, pid, name, age):
    # TODO: ADD TO SQL
    app.create_person(pid, name)

def add_person_contact(app, pid1, pid2):
    app.create_person_contact(pid1, pid2)

def add_covid_strain(app, sid, name, data):
    # TODO: ADD TO SQL
    app.create_covid_strain(sid, name)

def add_vaccine(app, vid, name, data):
    # TODO: ADD TO SQL
    app.create_vaccine(vid, name)

def delete_person(app, pid):
    app.delete_person(pid)
    # TODO: DELETE FROM SQL

def add_city_person_rel(app, cid, pid):
    app.create_city_person_relation(cid, pid)

def add_country_city_rel(app, coid, ciid):
    app.create_country_city_relation(coid, ciid)

def modify_residence(app, pid, ncid):
    app.change_person_city(pid, ncid)

def populator(app):
    # CLEAR DB
    clear_db(app)

    # CREATE COUNTRIES
    add_country(app, 1, "Spain without the S", 1)

    # CREATE CITIES
    add_city(app, 1, "Barcelona", 1)
    add_city(app, 2, "Madrid", 1)

    # CREATE PERSONS
    add_person(app, 1, "David", 12)
    add_person(app, 2, "Alberto", 34)
    add_person(app, 3, "Samuel", 56)
    add_person(app, 4, "Eric", 78)

    # CREATE COVID STRAINS
    add_covid_strain(app, 1, "Alpha", 1)
    add_covid_strain(app, 2, "Beta", 1)

    # CREATE VACCINE
    add_vaccine(app, 1, "Astrazeneca", 1)
    add_vaccine(app, 2, "Moderna", 1)

    # CREATE PERSONS CONTACTS
    add_person_contact(app, 1, 2)
    add_person_contact(app, 3, 4)

    # CREATE PERSON-CITY RELATIONS
    add_city_person_rel(app, 1, 1)
    add_city_person_rel(app, 1, 2)

    # CREATE COUNTRY-CITY RELATIONS
    add_country_city_rel(app, 1, 1)
    add_country_city_rel(app, 1, 2)

    # CREATE PERSON-STRAIN RELATION

    # CREATE PERSON-VACCINE RELATION

    # OTHER QUERIES
    modify_residence(app, 1, 2)
    delete_person(app, 3)

if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "neo4j+s://10133952.databases.neo4j.io"
    user = "neo4j"
    password = "bwlRM9QFS7BleaZDEX_fyxXaKprfuO2Oyl9U0lQ-rVc"
    app = App(uri, user, password)
    populator(app)
    app.close()