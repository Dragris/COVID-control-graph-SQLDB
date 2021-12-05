from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

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