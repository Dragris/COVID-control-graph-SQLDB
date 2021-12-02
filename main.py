
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

def delete_person(app, pid):
    app.delete_person(pid)
    # TODO: DELETE FROM SQL

def add_city_person_rel(app, cid, pid):
    app.create_city_person_relation(cid, pid)

def add_country_city_rel(app, coid, ciid):
    app.create_country_city_relation(coid, ciid)


def populator(app):
    clear_db(app)
    add_country(app, 1, "Spain without the S", 1)
    add_city(app, 1, "Barcelona", 1)
    add_person(app, 1, "David", 12)
    add_person(app, 2, "Alberto", 34)
    add_person_contact(app, 1, 2)
    add_person(app, 3, "Samuel", 56)
    add_person(app, 4, "Eric", 78)
    add_person_contact(app, 3, 4)
    add_city_person_rel(app, 1, 1)
    add_country_city_rel(app, 1, 1)
    delete_person(app, 3)

if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "neo4j+s://10133952.databases.neo4j.io"
    user = "neo4j"
    password = "bwlRM9QFS7BleaZDEX_fyxXaKprfuO2Oyl9U0lQ-rVc"
    app = App(uri, user, password)
    populator(app)
    app.close()