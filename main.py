
from test import App


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = "neo4j+s://10133952.databases.neo4j.io"
    user = "neo4j"
    password = "bwlRM9QFS7BleaZDEX_fyxXaKprfuO2Oyl9U0lQ-rVc"
    app = App(uri, user, password)
    app.create_person("David")
    app.create_person("Alberto")
    app.find_person("David")
    app.close()