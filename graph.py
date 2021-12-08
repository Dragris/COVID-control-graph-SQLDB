from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import numpy as np

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def create_country(self, country_id, country_name):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_country, country_id, country_name
            )
            print("Created COUNTRY with ID: {0} and NAME: {1}".format(country_id, country_name))

    @staticmethod
    def _create_country(tx, country_id, country_name):
        query = (
            "CREATE (c:Country { country_id: $country_id, name: $country_name }) "
            "RETURN c "
        )
        result = tx.run(query, country_id=country_id, country_name=country_name)
        try:
            return [{"c": row["c"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_country_city_relation(self, country_id, city_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_country_city_relation, country_id, city_id
            )
            print("Created relation: {0} CONTAINS {1}".format(result[0]['co'], result[0]['ci']))

    @staticmethod
    def _create_country_city_relation(tx, country_id, city_id):
        query = (
            "MATCH (co:Country {country_id: $country_id}) "
            "MATCH (ci:City {city_id: $city_id}) "
            "CREATE (co)-[:CONTAINS]->(ci) "
            "RETURN co, ci "
        )

        result = tx.run(query, country_id=country_id, city_id=city_id)
        try:
            return [{"co": row["co"]["name"], "ci": row["ci"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_city(self, city_id, city_name):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_city, city_id, city_name
            )
            print("Created CITY with ID: {0} and NAME: {1}".format(city_id, city_name))

    @staticmethod
    def _create_city(tx, city_id, city_name):
        query = (
            "CREATE (c:City { city_id: $city_id, name: $city_name }) "
            "RETURN c"
        )
        result = tx.run(query, city_id=city_id, city_name=city_name)
        try:
            return [{"c": row["c"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_city_person_relation(self, city_id, person_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_city_person_relation, city_id, person_id
            )
            print("Created relation: {0} IS RESIDED BY {1}".format(result[0]['c'], result[0]['p']))

    @staticmethod
    def _create_city_person_relation(tx, city_id, person_id):
        query = (
            "MATCH (c:City {city_id: $city_id}) "
            "MATCH (p:Person {person_id: $person_id}) "
            "CREATE (c)-[:RESIDED_BY]->(p) "
            "RETURN c, p "
        )

        result = tx.run(query, city_id=city_id, person_id=person_id)
        try:
            return [{"c": row["c"]["name"], "p": row["p"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_person(self, person_id, person_name):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_person, person_id, person_name
            )
            print("Created PERSON with ID: {0} and NAME: {1}".format(person_id, person_name))

    @staticmethod
    def _create_person(tx, person_id, person_name):
        query = (
            "CREATE (p:Person { person_id: $person_id, name: $person_name }) "
            "RETURN p"
        )
        result = tx.run(query, person_id=person_id, person_name=person_name)
        try:
            return [{"p": row["p"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def delete_person(self, person_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._delete_person, person_id
            )
            print("Deleted PERSON with ID: {0}".format(person_id))

    @staticmethod
    def _delete_person(tx, person_id):
        query = (
            "MATCH (p:Person {person_id: $person_id}) "
            "DETACH DELETE p "
        )
        result = tx.run(query, person_id=person_id)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_person_contact(self, person1_id, person2_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_person_contact, person1_id, person2_id
            )
            print("Created relation: {0} HAS BEEN WITH {1}".format(result[0]['p1'], result[0]['p2']))

    @staticmethod
    def _create_person_contact(tx, person1_id, person2_id):
        query = (
            "MATCH (p1:Person {person_id: $person1_id}) "
            "MATCH (p2:Person {person_id: $person2_id}) "
            "CREATE (p1)-[:HAS_BEEN_WITH]->(p2) "
            "CREATE (p2)-[:HAS_BEEN_WITH]->(p1) "
            "RETURN p1, p2 "
        )

        result = tx.run(query, person1_id=person1_id, person2_id=person2_id)
        try:
            return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def change_person_city(self, person_id, new_city_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._change_person_city, person_id, new_city_id
            )
            print("Modified relation {0} IS RESIDED BY {1} to {2} IS RESIDED BY {1}".format(result[0]['c1'], result[0]['p'], result[0]['c2']))

    @staticmethod
    def _change_person_city(tx, person_id, new_city_id):
        query = (
            "MATCH (p:Person {person_id: $person_id}) "
            "MATCH (c2:City {city_id: $new_city_id}) "
            "MATCH (c1:City) - [r] -> (p) "
            "DETACH DELETE r "
            "CREATE (c2) - [:RESIDED_BY] -> (p) "
            "RETURN p, c1, c2 "
        )

        result = tx.run(query, person_id=person_id, new_city_id=new_city_id)
        try:
            return [{"p": row["p"]["name"], "c1": row["c1"]["name"], "c2": row["c2"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def delete_all(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._delete_all
            )
            print("Graph DB cleared")

    @staticmethod
    def _delete_all(tx):
        query = (
            "MATCH (p) "
            "DETACH DELETE p "
        )
        result = tx.run(query)
        try:
            return ["Graph DB cleared"]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_covid_strain(self, strain_id, strain_name):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_covid_strain, strain_id, strain_name
            )
            print("Created COVID STRAIN with ID: {0} and NAME: {1}".format(strain_id, strain_name))

    @staticmethod
    def _create_covid_strain(tx, strain_id, strain_name):
        query = (
            "CREATE (s:Strain { strain_id: $strain_id, name: $strain_name }) "
            "RETURN s"
        )
        result = tx.run(query, strain_id=strain_id, strain_name=strain_name)
        try:
            return [{"s": row["s"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_vaccine(self, vac_id, vac_name):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_vaccine, vac_id, vac_name
            )
            print("Created VACCINE with ID: {0} and NAME: {1}".format(vac_id, vac_name))

    @staticmethod
    def _create_vaccine(tx, vac_id, vac_name):
        query = (
            "CREATE (v:Vaccine { vac_id: $vac_id, name: $vac_name }) "
            "RETURN v"
        )
        result = tx.run(query, vac_id=vac_id, vac_name=vac_name)
        try:
            return [{"v": row["v"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_infected_relation(self, person_id, strain_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_infected_relation, person_id, strain_id
            )
            print("Created relation: {0} IS INFECTED BY {1}".format(result[0]['p'], result[0]['s']))

    @staticmethod
    def _create_infected_relation(tx, person_id, strain_id):
        query = (
            "MATCH (p:Person {person_id: $person_id}) "
            "MATCH (s:Strain {strain_id: $strain_id}) "
            "CREATE (p)-[:INFECTED_BY]->(s) "
            "RETURN p, s "
        )

        result = tx.run(query, person_id=person_id, strain_id=strain_id)
        try:
            return [{"p": row["p"]["name"], "s": row["s"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_vaccinated_relation(self, person_id, vac_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_vaccinated_relation, person_id, vac_id
            )
            print("Created relation: {0} IS VACCINATED WITH {1}".format(result[0]['p'], result[0]['v']))

    @staticmethod
    def _create_vaccinated_relation(tx, person_id, vac_id):
        query = (
            "MATCH (p:Person {person_id: $person_id}) "
            "MATCH (v:Vaccine {vac_id: $vac_id}) "
            "CREATE (p)-[:VACCCINATED_WITH]->(v) "
            "RETURN p, v "
        )

        result = tx.run(query, person_id=person_id, vac_id=vac_id)
        try:
            return [{"p": row["p"]["name"], "v": row["v"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def delete_infected_relation(self, person_id, strain_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._delete_infected_relation, person_id, strain_id
            )
            print("Deleted relation: {0} IS INFECTED BY {1}".format(result[0]['p'], result[0]['s']))

    @staticmethod
    def _delete_infected_relation(tx, person_id, strain_id):
        query = (
            "MATCH (p:Person {person_id: $person_id}) "
            "MATCH (s:Strain {strain_id: $strain_id}) "
            "MATCH (p)-[r]->(s) "
            "DETACH DELETE r "
            "RETURN p, s "
        )

        result = tx.run(query, person_id=person_id, strain_id=strain_id)
        try:
            return [{"p": row["p"]["name"], "s": row["s"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def is_person_infected(self, person_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._is_person_infected, person_id
            )
            if result:
                print("{0} is infected by {1} strains".format(result[0]['p'], result[0]['n_strains']))
                return True
            else:
                print("PERSON with ID: {0} is not infected".format(person_id))
                return False

    @staticmethod
    def _is_person_infected(tx, person_id):
        query = (
            "MATCH (p:Person {person_id: $person_id}) "
            "MATCH (p)-[r]->(:Strain) "
            "RETURN count(r) as n_strains, p "
        )

        result = tx.run(query, person_id=person_id)
        try:
            return [{"n_strains": row["n_strains"], 'p': row['p']['name']} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def which_strain(self, person_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._which_strain, person_id
            )
            ret = []
            for row in result:
                print("PERSON with ID {0} is infected by: {1}".format(person_id, row))
                ret.append(row)
            return ret

    @staticmethod
    def _which_strain(tx, person_id):
        query = (
            "MATCH (p:Person {person_id: $person_id}) "
            "MATCH (p)-[r]->(s:Strain) "
            "RETURN s.name AS name "
        )
        result = tx.run(query, person_id=person_id)
        try:
            return [row["name"] for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def is_person_vaccinated(self, person_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._is_person_vaccinated, person_id
            )
            if result:
                print("{0} is vaccinated with {1} different vaccines".format(result[0]['p'], result[0]['n_vaccines']))
                return True
            else:
                print("PERSON with ID: {0} is not vaccinated".format(person_id))
                return False

    @staticmethod
    def _is_person_vaccinated(tx, person_id):
        query = (
            "MATCH (p:Person {person_id: $person_id}) "
            "MATCH (p)-[r]->(:Vaccine) "
            "RETURN count(r) as n_vaccines, p "
        )

        result = tx.run(query, person_id=person_id)
        try:
            return [{"n_vaccines": row["n_vaccines"], 'p': row['p']['name']} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def which_vaccine(self, person_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._which_vaccine, person_id
            )
            ret = []
            for row in result:
                print("PERSON with ID {0} is vaccinated with: {1}".format(person_id, row))
                ret.append(row)
            return ret

    @staticmethod
    def _which_vaccine(tx, person_id):
        query = (
            "MATCH (p:Person {person_id: $person_id}) "
            "MATCH (p)-[r]->(v:Vaccine) "
            "RETURN v.name AS name "
        )
        result = tx.run(query, person_id=person_id)
        try:
            return [row["name"] for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def has_city_infected(self, city_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._has_city_infected, city_id
            )
            if result:
                print("{0} has {1} infected person/s".format(result[0]['c'], result[0]['n_infected']))
                return True, result[0]['n_infected']
            else:
                print("CITY with ID: {0} has no infected person".format(city_id))
                return False, 0

    @staticmethod
    def _has_city_infected(tx, city_id):
        query = (
            "MATCH (c:City {city_id: $city_id}) "
            "MATCH (c) - [] -> (p:Person) - [] -> (:Strain) "
            "RETURN count(DISTINCT p) as n_infected, c "
        )

        result = tx.run(query, city_id=city_id)
        try:
            return [{"n_infected": row["n_infected"], 'c': row['c']['name']} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def has_city_vaccinated(self, city_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._has_city_vaccinated, city_id
            )
            if result:
                print("{0} has {1} vaccinated person/s".format(result[0]['c'], result[0]['n_vaccines']))
                return True, result[0]['n_vaccines']
            else:
                print("CITY with ID: {0} has no vaccinated person".format(city_id))
                return False, 0

    @staticmethod
    def _has_city_vaccinated(tx, city_id):
        query = (
            "MATCH (c:City {city_id: $city_id}) "
            "MATCH (c) - [] -> (p:Person) - [] -> (:Vaccine) "
            "RETURN count(DISTINCT p) as n_vaccines, c "
        )

        result = tx.run(query, city_id=city_id)
        try:
            return [{"n_vaccines": row["n_vaccines"], "c": row["c"]["name"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def total_vaccinated(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._total_vaccinated
            )
            if result:
                print("There are {0} vaccinated person/s".format(result[0]['n_vaccines']))
                return result[0]['n_vaccines']

    @staticmethod
    def _total_vaccinated(tx):
        query = (
            "MATCH (p:Person) - [] -> (:Vaccine) "
            "RETURN count(DISTINCT p) as n_vaccines "
        )

        result = tx.run(query)
        try:
            return [{"n_vaccines": row["n_vaccines"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def total_infected(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._total_infected
            )
            if result:
                print("There are {0} infected person/s".format(result[0]['n_infect']))
                return result[0]['n_infect']

    @staticmethod
    def _total_infected(tx):
        query = (
            "MATCH (p:Person) - [] -> (:Strain) "
            "RETURN count(DISTINCT p) as n_infect "
        )

        result = tx.run(query)
        try:
            return [{"n_infect": row["n_infect"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def city_most_infected(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._city_most_infected
            )
            if result:
                print("{0} is the most infected city with {1} infected person/s".format(result[0]['c'], result[0]['num']))
                return result[0]['c'], result[0]['num']

    @staticmethod
    def _city_most_infected(tx):
        query = (
            "MATCH (c:City)-[r]->(p:Person) - [] -> (:Strain) "
            "RETURN c, count(DISTINCT r) AS num "
            "ORDER BY num DESC LIMIT 1 "
        )

        result = tx.run(query)
        try:
            return [{"c": row["c"]["name"], "num": row["num"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def country_most_infected(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._country_most_infected
            )
            if result:
                print("{0} is the most infected country with {1} infected person/s".format(result[0]['c'], result[0]['num']))
                return result[0]['c'], result[0]['num']

    @staticmethod
    def _country_most_infected(tx):
        query = (
            "MATCH (c:Country) - [] ->(:City) - [r] -> (p:Person) - [] -> (:Strain) "
            "RETURN c, count(DISTINCT r) AS num "
            "ORDER BY num DESC LIMIT 1 "
        )

        result = tx.run(query)
        try:
            return [{"c": row["c"]["name"], "num": row["num"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def city_most_vaccinated(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._city_most_vaccinated
            )
            if result:
                print("{0} is the most vaccinated city with {1} vaccinated person/s".format(result[0]['c'], result[0]['num']))
                return result[0]['c'], result[0]['num']

    @staticmethod
    def _city_most_vaccinated(tx):
        query = (
            "MATCH (c:City)-[r]->(p:Person) - [] -> (:Vaccine) "
            "RETURN c, count(DISTINCT r) AS num "
            "ORDER BY num DESC LIMIT 1 "
        )

        result = tx.run(query)
        try:
            return [{"c": row["c"]["name"], "num": row["num"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def country_most_vaccinated(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._country_most_vaccinated
            )
            if result:
                print("{0} is the most vaccinated country with {1} vaccinated person/s".format(result[0]['c'], result[0]['num']))
                return result[0]['c'], result[0]['num']

    @staticmethod
    def _country_most_vaccinated(tx):
        query = (
            "MATCH (c:Country) - [] ->(:City) - [r] -> (p:Person) - [] -> (:Vaccine) "
            "RETURN c, count(DISTINCT r) AS num "
            "ORDER BY num DESC LIMIT 1 "
        )

        result = tx.run(query)
        try:
            return [{"c": row["c"]["name"], "num": row["num"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def city_least_infected(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._city_least_infected
            )
            if result:
                infected_cities = result[0]['ic']
                city_list = result[0]['city_list']
                nums =  result[0]['nums']
                tmp = (None, np.Inf)
                if len(infected_cities) == len(city_list):
                    for city_id in range(len(infected_cities)):
                        if nums[city_id] <= tmp[1]:
                            tmp = (infected_cities[city_id]['name'], nums[city_id])
                else:
                    for city_id in range(len(city_list)):
                        if city_list[city_id] not in infected_cities:
                            tmp = (city_list[city_id]['name'], 0)
                print("{0} is the least infected city with {1} infected person/s".format(tmp[0], tmp[1]))
                return tmp

    @staticmethod
    def _city_least_infected(tx):
        query = (
            "MATCH (ci:City) "
            "MATCH (c:City)-[r]->(p:Person) - [] -> (:Strain) "
            "WITH DISTINCT c AS infected_cities, count(DISTINCT r) as num, collect(DISTINCT ci) as city_list "
            "RETURN collect(infected_cities) as infected_list, collect(num) AS nums, city_list "
            "ORDER BY nums "
        )

        result = tx.run(query)
        try:
            return [{"ic": row["infected_list"], "nums": row["nums"], "city_list": row["city_list"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def country_least_infected(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._country_least_infected
            )
            if result:
                infected_countries = result[0]['ic']
                country_list = result[0]['country_list']
                nums =  result[0]['nums']
                tmp = (None, np.Inf)
                if len(infected_countries) == len(country_list):
                    for country_id in range(len(infected_countries)):
                        if nums[country_id] <= tmp[1]:
                            tmp = (infected_countries[country_id]['name'], nums[country_id])
                else:
                    for country_id in range(len(country_list)):
                        if country_list[country_id] not in infected_countries:
                            tmp = (country_list[country_id]['name'], 0)
                print("{0} is the least infected country with {1} infected person/s".format(tmp[0], tmp[1]))
                return tmp

    @staticmethod
    def _country_least_infected(tx):
        query = (
            "MATCH (co:Country) "
            "MATCH (c:Country) - [] -> (:City)-[r]->(p:Person) - [] -> (:Strain) "
            "WITH DISTINCT c AS infected_countries, count(DISTINCT r) as num, collect(DISTINCT co) as country_list "
            "RETURN collect(infected_countries) as infected_list, collect(num) AS nums, country_list "
            "ORDER BY nums "
        )

        result = tx.run(query)
        try:
            return [{"ic": row["infected_list"], "nums": row["nums"], "country_list": row["country_list"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def city_least_vaccinated(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._city_least_vaccinated
            )
            if result:
                vaccinated_cities = result[0]['vc']
                city_list = result[0]['city_list']
                nums =  result[0]['nums']
                tmp = (None, np.Inf)
                if len(vaccinated_cities) == len(city_list):
                    for city_id in range(len(vaccinated_cities)):
                        if nums[city_id] <= tmp[1]:
                            tmp = (vaccinated_cities[city_id]['name'], nums[city_id])
                else:
                    for city_id in range(len(city_list)):
                        if city_list[city_id] not in vaccinated_cities:
                            tmp = (city_list[city_id]['name'], 0)
                print("{0} is the least vaccinated city with {1} vaccinated person/s".format(tmp[0], tmp[1]))
                return tmp

    @staticmethod
    def _city_least_vaccinated(tx):
        query = (
            "MATCH (ci:City) "
            "MATCH (c:City)-[r]->(p:Person) - [] -> (:Vaccine) "
            "WITH DISTINCT c AS vaccinated_cities, count(DISTINCT r) as num, collect(DISTINCT ci) as city_list "
            "RETURN collect(vaccinated_cities) as vaccinated_list, collect(num) AS nums, city_list "
            "ORDER BY nums "
        )

        result = tx.run(query)
        try:
            return [{"vc": row["vaccinated_list"], "nums": row["nums"], "city_list": row["city_list"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def country_least_vaccinated(self):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._country_least_vaccinated
            )
            if result:
                vaccinated_countries = result[0]['vc']
                country_list = result[0]['country_list']
                nums =  result[0]['nums']
                tmp = (None, np.Inf)
                if len(vaccinated_countries) == len(country_list):
                    for country_id in range(len(vaccinated_countries)):
                        if nums[country_id] <= tmp[1]:
                            tmp = (vaccinated_countries[country_id]['name'], nums[country_id])
                else:
                    for country_id in range(len(country_list)):
                        if country_list[country_id] not in vaccinated_countries:
                            tmp = (country_list[country_id]['name'], 0)
                print("{0} is the least vaccinated country with {1} vaccinated person/s".format(tmp[0], tmp[1]))
                return tmp

    @staticmethod
    def _country_least_vaccinated(tx):
        query = (
            "MATCH (co:Country) "
            "MATCH (c:Country) - [] -> (:City)-[r]->(p:Person) - [] -> (:Vaccine) "
            "WITH DISTINCT c AS vaccinated_countries, count(DISTINCT r) as num, collect(DISTINCT co) as country_list "
            "RETURN collect(vaccinated_countries) as vaccinated_list, collect(num) AS nums, country_list "
            "ORDER BY nums "
        )

        result = tx.run(query)
        try:
            return [{"vc": row["vaccinated_list"], "nums": row["nums"], "country_list": row["country_list"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise


    def person_distance_to_virus(self, person_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._person_distance_to_virus, person_id
            )
            jumps = np.Inf if result[0]['jtv'] is None else result[0]['jtv']
            print("{0} is at {1} jumps from the virus".format(result[0]['p'], jumps))
            return jumps

    @staticmethod
    def _person_distance_to_virus(tx, person_id):
        query = (
            "MATCH (p1:Person {person_id: $person_id}) "
            "MATCH (p2:Person) - [] -> (:Strain) "
            "RETURN min(length(shortestPath((p1) - [*..15] -> (p2)))) as jumps_to_virus, p1 "
        )
        result = tx.run(query, person_id=person_id)
        try:
            return [{"jtv": row["jumps_to_virus"], "p": row['p1']['name']} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def person_contact_to_virus(self, person_id):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._person_contact_to_virus, person_id
            )
            if result:
                print("{0} is in contact with the virus".format(result[0]['p1']))
                return True
            else:
                print("PERSON with ID: {0} is either not in contact with the virus or infected".format(person_id))
                return False

    @staticmethod
    def _person_contact_to_virus(tx, person_id):
        query = (
            "MATCH (p1:Person {person_id: $person_id}) - [] -> (p2:Person) - [] -> (:Strain) "
            "RETURN DISTINCT p2, p1 "
        )
        result = tx.run(query, person_id=person_id)
        try:
            return [{"p1": row["p1"]["name"], "p2": row['p2']['name']} for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    """
    
"""