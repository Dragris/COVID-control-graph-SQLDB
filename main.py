from graph import App
import mysql
import MySQLdb


def clear_db(app, conn):
    mysql.delete_all(conn)
    app.delete_all()


def add_country(app, conn, cid, name, population):
    # TODO: ADD TO SQL
    cid = mysql.add_country(conn, name, population)
    app.create_country(cid, name)


def add_city(app, conn, cid, name, population):
    # TODO: ADD TO SQL
    app.create_city(cid, name)


def add_person(app, conn, pid, name, age):
    # TODO: ADD TO SQL
    app.create_person(pid, name)


def add_person_contact(app, conn, pid1, pid2):
    app.create_person_contact(pid1, pid2)


def add_covid_strain(app, conn, sid, name, data):
    # TODO: ADD TO SQL
    app.create_covid_strain(sid, name)


def add_vaccine(app, conn, vid, name, data):
    # TODO: ADD TO SQL
    app.create_vaccine(vid, name)


def delete_person(app, conn, pid):
    app.delete_person(pid)
    # TODO: DELETE FROM SQL


def add_city_person_rel(app, conn, cid, pid):
    app.create_city_person_relation(cid, pid)


def add_country_city_rel(app, conn, coid, ciid):
    app.create_country_city_relation(coid, ciid)


def modify_residence(app, conn, pid, ncid):
    app.change_person_city(pid, ncid)


def add_infected_relation(app, conn, pid, sid):
    app.create_infected_relation(pid, sid)


def add_vaccinated_relation(app, conn, pid, vid):
    app.create_vaccinated_relation(pid, vid)


def delete_infected_relation(app, conn, pid, sid):
    app.delete_infected_relation(pid, sid)


def is_person_infected(app, conn, pid):
    return app.is_person_infected(pid)


def which_strain(app, conn, pid):
    return app.which_strain(pid)


def populator(app, conn):
    # CLEAR DB
    clear_db(app, conn)

    # CREATE COUNTRIES
    add_country(app, conn, 1, "Spain without the S", 1)

    # CREATE CITIES
    add_city(app, conn, 1, "Barcelona", 1)
    add_city(app, conn, 2, "Madrid", 1)

    # CREATE PERSONS
    add_person(app, conn, 1, "David", 12)
    add_person(app, conn, 2, "Alberto", 34)
    add_person(app, conn, 3, "Samuel", 56)
    add_person(app, conn, 4, "Eric", 78)
    add_person(app, conn, 5, "Jerónimo", 91)

    # CREATE COVID STRAINS
    add_covid_strain(app, conn, 1, "Alpha", 1)
    add_covid_strain(app, conn, 2, "Beta", 1)

    # CREATE VACCINE
    add_vaccine(app, conn, 1, "Astrazeneca", 1)
    add_vaccine(app, conn, 2, "Moderna", 1)

    # CREATE PERSONS CONTACTS
    add_person_contact(app, conn, 1, 2)
    add_person_contact(app, conn, 3, 4)
    add_person_contact(app, conn, 4, 5)

    # CREATE PERSON-CITY RELATIONS
    add_city_person_rel(app, conn, 1, 1)
    add_city_person_rel(app, conn, 1, 2)

    # CREATE COUNTRY-CITY RELATIONS
    add_country_city_rel(app, conn, 1, 1)
    add_country_city_rel(app, conn, 1, 2)

    # CREATE PERSON-STRAIN RELATION
    add_infected_relation(app, conn, 1, 1)
    add_infected_relation(app, conn, 1, 2)
    add_infected_relation(app, conn, 4, 2)

    # CREATE PERSON-VACCINE RELATION
    add_vaccinated_relation(app, conn, 2, 1)
    add_vaccinated_relation(app, conn, 5, 2)

    # MODIFY PERSON RESIDENCE
    modify_residence(app, conn, 1, 2)

    # DELETE PERSON
    delete_person(app, conn, 3)

    # DELETE INFECTED RELATION
    delete_infected_relation(app, conn, 1, 2)

    # IS A PERSON INFECTED? HOW MANY SRTAINS?
    is_person_infected(app, conn, 1)
    delete_infected_relation(app, conn, 1, 1)
    is_person_infected(app, conn, 1)

    # BY WHICH STRAIN/S IS A PERSON INFECTED
    is_person_infected(app, conn, 4)
    which_strain(app, conn, 4)


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "neo4j+s://10133952.databases.neo4j.io"
    user = "neo4j"
    password = "bwlRM9QFS7BleaZDEX_fyxXaKprfuO2Oyl9U0lQ-rVc"
    pass_sql = 'pon aqui tu contra bro'
    conn = MySQLdb.connect(host='localhost', user='root', passwd=pass_sql, db='COVID')
    app = App(uri, user, password)

    populator(app, conn)

    app.close()
    conn.close()
