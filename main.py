from graph import App
import mysql
import MySQLdb
import consts


def clear_db(app, conn):
    mysql.delete_all(conn)
    app.delete_all()


def add_country(app, conn, name, population):
    try:
        cid = mysql.add_country(conn, name, population)
        app.create_country(cid, name)
    except:
        conn.rollback()
        return

    conn.commit()


def add_city(app, conn, cid, name, population):
    try:
        cid = mysql.add_city(conn, name, population)
        app.create_city(cid, name)
    except:
        conn.rollback()
        return

    conn.commit()


def add_person(app, conn, pid="12345678A", first_name="John", last_name="Doe", sex="N", age="2000-01-01 09:00:00"):
    try:
        uid = mysql.add_person(conn, first_name, last_name, pid, sex, age)
        app.create_person(uid, first_name)
    except:
        conn.rollback()
        return

    conn.commit()


def add_person_contact(app, pid1, pid2):
    app.create_person_contact(pid1, pid2)


def add_covid_strain(app, conn, sid, name, data):
    try:
        sid = mysql.add_covid_strain(conn, name, data)
        app.create_covid_strain(sid, name)
    except:
        conn.rollback()
        return

    conn.commit()


def add_vaccine(app, conn, vid, name, data):
    try:
        vid = mysql.add_vaccine(conn, name, data)
        app.create_vaccine(vid, name)
    except:
        conn.rollback()
        return

    conn.commit()


def delete_person(app, conn, pid):
    app.delete_person(pid)
    mysql.delete_person(conn, pid)


def add_city_person_rel(app, cid, pid):
    app.create_city_person_relation(cid, pid)


def add_country_city_rel(app, coid, ciid):
    app.create_country_city_relation(coid, ciid)


def modify_residence(app, pid, ncid):
    app.change_person_city(pid, ncid)


def add_infected_relation(app, pid, sid):
    app.create_infected_relation(pid, sid)


def add_vaccinated_relation(app, pid, vid):
    app.create_vaccinated_relation(pid, vid)


def delete_infected_relation(app, pid, sid):
    app.delete_infected_relation(pid, sid)


def is_person_infected(app, pid):
    return app.is_person_infected(pid)


def which_strain(app, pid):
    return app.which_strain(pid)


def is_person_vaccinated(app, conn, pid):
    return app.is_person_vaccinated(pid)


def which_vaccine(app, pid):
    return app.which_vaccine(pid)


def has_city_infected(app, conn, cid):
    return app.has_city_infected(cid)


def has_city_vaccinated(app, conn, cid):
    return app.has_city_vaccinated(cid)


def populator(app, conn):
    # CLEAR DB
    print('Clearing DB')
    clear_db(app, conn)

    # CREATE COUNTRIES
    print('\nCreating countries')
    add_country(app, conn, "Spain without the S", 1)

    # CREATE CITIES
    print('\nCreating cities')
    add_city(app, conn, 1, "Barcelona", 1)
    add_city(app, conn, 2, "Madrid", 1)

    # CREATE PERSONS
    print('\nCreating persons')
    add_person(app, conn, 1, "David", 12)
    add_person(app, conn, 2, "Alberto", 34)
    add_person(app, conn, 3, "Samuel", 56)
    add_person(app, conn, 4, "Eric", 78)
    add_person(app, conn, 5, "Jerónimo", 91)

    # CREATE COVID STRAINS
    print('\nCreating strains')
    add_covid_strain(app, conn, 1, "Alpha", 1)
    add_covid_strain(app, conn, 2, "Beta", 1)

    # CREATE VACCINE
    print('\nCreating vaccines')
    add_vaccine(app, conn, 1, "Astrazeneca", 1)
    add_vaccine(app, conn, 2, "Moderna", 1)

    # CREATE PERSONS CONTACTS
    print('\nCreating PERSON-PERSON relations')
    add_person_contact(app, 1, 2)
    add_person_contact(app, 3, 4)
    add_person_contact(app, 4, 5)

    # CREATE PERSON-CITY RELATIONS
    print('\nCreating PERSON-CITY relations')
    add_city_person_rel(app, 1, 1)
    add_city_person_rel(app, 1, 2)

    # CREATE COUNTRY-CITY RELATIONS
    print('\nCreating COUNTRY-CITY relations')
    add_country_city_rel(app, 1, 1)
    add_country_city_rel(app, 1, 2)

    # CREATE PERSON-STRAIN RELATION
    print('\nCreating PERSON-STRAIN relations')
    add_infected_relation(app, 1, 1)
    add_infected_relation(app, 1, 2)
    add_infected_relation(app, 4, 1)
    add_infected_relation(app, 4, 2)

    # CREATE PERSON-VACCINE RELATION
    print('\nCreating PERSON-VACCINE relations')
    add_vaccinated_relation(app, 2, 1)
    add_vaccinated_relation(app, 2, 2)
    add_vaccinated_relation(app, 5, 2)

    # MODIFY PERSON RESIDENCE
    print('\nModifying person residence')
    modify_residence(app, 1, 2)

    # DELETE PERSON
    print('\nDeleting person')
    delete_person(app, conn, 3)

    # DELETE INFECTED RELATION
    print('\nDeleting PERSON-STRAIN relation')
    delete_infected_relation(app, 1, 2)

    # IS A PERSON INFECTED? HOW MANY SRTAINS?
    print('\nQuery: Is a person infected? By how many strains?')
    is_person_infected(app, 1)
    is_person_infected(app, 4)

    # BY WHICH STRAIN/S IS A PERSON INFECTED?
    print('\nQuery: By which strain/s is a person infected?')
    which_strain(app, 4)

    # IS A PERSON VACCINATED? HOW MANY VACCINES?
    print('\nQuery: Is a person vaccinated? With how many vaccines?')
    is_person_vaccinated(app, 2)
    is_person_vaccinated(app, 1)

    # WITH WHAT VACCINE/S IS A PERSON VACCINATED?
    print('\nQuery: With what vaccine/s is a person vaccinated?')
    which_vaccine(app, 2)

    # HAS A CITY ANY INFECTED PERSON? HOW MANY?
    print('\nQuery: How many infected persons are in a city?')
    has_city_infected(app, conn, 1)
    has_city_infected(app, conn, 2)

    # HAS A CITY ANY VACCINATED PERSON? HOW MANY?
    print('\nQuery: How many infected persons are in a city?')
    has_city_vaccinated(app, conn, 1)
    has_city_vaccinated(app, conn, 2)


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    conn = MySQLdb.connect(host=consts.HOST, user=consts.SQL_USER, passwd=consts.SQL_PASS, db=consts.DB)
    app = App(consts.URI, consts.GRAPH_USER, consts.GRAPH_PASS)

    populator(app, conn)

    app.close()
    conn.close()
