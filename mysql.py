from datetime import datetime


def delete_all(conn):
    cur = conn.cursor()
    try:
        cur.execute("SET SQL_SAFE_UPDATES = 0")

        cur.execute("DROP TABLE person")
        cur.execute(
            "CREATE TABLE person (`idperson` int NOT NULL AUTO_INCREMENT, `first_name` varchar(45) NOT NULL, `last_name` varchar(45) NOT NULL, `pid` varchar(9) NOT NULL, `sex` char(1) NOT NULL, `dob` datetime NOT NULL, PRIMARY KEY (`idperson`), UNIQUE KEY `idperson_UNIQUE` (`idperson`))"
        )

        cur.execute("DROP TABLE city")
        cur.execute(
            "CREATE TABLE city (`idcity` int NOT NULL AUTO_INCREMENT, `name` varchar(45) NOT NULL, `population` int unsigned NOT NULL, PRIMARY KEY (`idcity`), UNIQUE KEY `idcity_UNIQUE` (`idcity`))"
        )

        cur.execute("DROP TABLE country")
        cur.execute(
            "CREATE TABLE country ( `idcountry` INT NOT NULL AUTO_INCREMENT, `name` VARCHAR(45) NOT NULL, `population` INT UNSIGNED NOT NULL, PRIMARY KEY (`idcountry`), UNIQUE INDEX `idcountry_UNIQUE` (`idcountry` ASC) VISIBLE, UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE)"
        )

        cur.execute("DROP TABLE vaccine")
        cur.execute(
            "CREATE TABLE vaccine (`idvaccine` int NOT NULL AUTO_INCREMENT, `name` varchar(45) NOT NULL, `description` varchar(500) DEFAULT NULL, PRIMARY KEY (`idvaccine`), UNIQUE KEY `idvaccine_UNIQUE` (`idvaccine`), UNIQUE KEY `name_UNIQUE` (`name`))"
        )

        cur.execute("DROP TABLE strain")
        cur.execute(
            "CREATE TABLE strain (`idstrain` int NOT NULL AUTO_INCREMENT, `name` varchar(45) NOT NULL, `description` varchar(500) DEFAULT NULL, PRIMARY KEY (`idstrain`), UNIQUE KEY `idstrain_UNIQUE` (`idstrain`), UNIQUE KEY `name_UNIQUE` (`name`))"
        )

        cur.execute("DROP TABLE stats")
        cur.execute(
            "CREATE TABLE stats (`idstats` int NOT NULL AUTO_INCREMENT, `type` varchar(45) NOT NULL, `output` varchar(1000) NOT NULL, `date` datetime NOT NULL, PRIMARY KEY (`idstats`), UNIQUE KEY `idstats_UNIQUE` (`idstats`));"
        )

        conn.commit()
    except:
        conn.rollback()
        raise
    print('SQL DB cleared')


def add_country(conn, name, population):
    cid = None
    cur = conn.cursor()
    try:
        sql = "INSERT INTO country VALUES (%s, %s, %s)"
        val = (None, name, population)
        cur.execute(sql, val)
        cid = cur.lastrowid
    except:
        raise

    return cid


def add_city(conn, name, population):
    cid = None
    cur = conn.cursor()
    try:
        sql = "INSERT INTO city VALUES (%s, %s, %s)"
        val = (None, name, population)
        cur.execute(sql, val)
        cid = cur.lastrowid
    except:
        raise

    return cid


def add_person(conn, first_name, last_name, pid, sex, age):
    uid = None
    cur = conn.cursor()
    try:
        sql = "INSERT INTO person VALUES (%s, %s, %s, %s, %s, %s)"
        val = (None, first_name, last_name, pid, sex, age)
        cur.execute(sql, val)
        uid = cur.lastrowid
    except:
        raise

    return uid


def add_covid_strain(conn, name, data):
    sid = None
    cur = conn.cursor()
    try:
        sql = "INSERT INTO strain VALUES (%s, %s, %s)"
        val = (None, name, data)
        cur.execute(sql, val)
        sid = cur.lastrowid
    except:
        raise

    return sid


def add_vaccine(conn, name, data):
    vid = None
    cur = conn.cursor()
    try:
        sql = "INSERT INTO vaccine VALUES (%s, %s, %s)"
        val = (None, name, data)
        cur.execute(sql, val)
        vid = cur.lastrowid
    except:
        raise

    return vid


def delete_person(conn, pid):
    try:
        cur = conn.cursor()
        sql = "DELETE FROM person WHERE idperson = %s"
        val = (pid,)  # Remember to put comma after only one val as it needs to be iterable
        cur.execute(sql, val)
    except:
        raise


def total_vaccinated(conn, n_vac):
    try:
        cur = conn.cursor()
        sql = "INSERT INTO stats VALUES (%s, %s, %s, %s)"
        val = (None, "totalVaccinated", n_vac, datetime.now())
        cur.execute(sql, val)
        conn.commit()
    except:
        conn.rollback()


def total_infected(conn, n_inf):
    try:
        cur = conn.cursor()
        sql = "INSERT INTO stats VALUES (%s, %s, %s, %s)"
        val = (None, "totalInfected", n_inf, datetime.now())
        cur.execute(sql, val)
        conn.commit()
    except:
        conn.rollback()


def most_infected_city(conn, city_name, num_infected):
    try:
        cur = conn.cursor()
        # Process output
        str_val = "City: " + city_name + "\nNumber of infected people: " + str(num_infected)

        sql = "INSERT INTO stats VALUES (%s, %s, %s, %s)"
        val = (None, "mostInfectedCity", str_val, datetime.now())
        cur.execute(sql, val)
        conn.commit()
    except:
        conn.rollback()


def most_infected_country(conn, country_name, num_infected):
    try:
        cur = conn.cursor()
        # Process output
        str_val = "Country: " + country_name + "\nNumber of infected people: " + str(num_infected)

        sql = "INSERT INTO stats VALUES (%s, %s, %s, %s)"
        val = (None, "mostInfectedCountry", str_val, datetime.now())
        cur.execute(sql, val)
        conn.commit()
    except:
        conn.rollback()


def most_vaccinated_city(conn, city_name, num_vaccinated):
    try:
        cur = conn.cursor()
        # Process output
        str_val = "City: " + city_name + "\nNumber of vaccinated people: " + str(num_vaccinated)

        sql = "INSERT INTO stats VALUES (%s, %s, %s, %s)"
        val = (None, "mostVaccinatedCity", str_val, datetime.now())
        cur.execute(sql, val)
        conn.commit()
    except:
        conn.rollback()


def most_vaccinated_country(conn, country_name, num_vaccinated):
    try:
        cur = conn.cursor()
        # Process output
        str_val = "Country: " + country_name + "\nNumber of vaccinated people: " + str(num_vaccinated)

        sql = "INSERT INTO stats VALUES (%s, %s, %s, %s)"
        val = (None, "mostVaccinatedCountry", str_val, datetime.now())
        cur.execute(sql, val)
        conn.commit()
    except:
        conn.rollback()


def least_infected_city(conn, city_name, num_vaccinated):
    try:
        cur = conn.cursor()
        # Process output
        str_val = "City: " + city_name + "\nNumber of infected people: " + str(num_vaccinated)

        sql = "INSERT INTO stats VALUES (%s, %s, %s, %s)"
        val = (None, "leastInfectedCity", str_val, datetime.now())
        cur.execute(sql, val)
        conn.commit()
    except:
        conn.rollback()


def least_infected_country(conn, country_name, num_vaccinated):
    try:
        cur = conn.cursor()
        # Process output
        str_val = "Country: " + country_name + "\nNumber of infected people: " + str(num_vaccinated)

        sql = "INSERT INTO stats VALUES (%s, %s, %s, %s)"
        val = (None, "leastInfectedCountry", str_val, datetime.now())
        cur.execute(sql, val)
        conn.commit()
    except:
        conn.rollback()


def least_vaccinated_city(conn, city_name, num_vaccinated):
    try:
        cur = conn.cursor()
        # Process output
        str_val = "City: " + city_name + "\nNumber of vaccinated people: " + str(num_vaccinated)

        sql = "INSERT INTO stats VALUES (%s, %s, %s, %s)"
        val = (None, "leastVaccinatedCity", str_val, datetime.now())
        cur.execute(sql, val)
        conn.commit()
    except:
        conn.rollback()


def least_vaccinated_country(conn, country_name, num_vaccinated):
    try:
        cur = conn.cursor()
        # Process output
        str_val = "Country: " + country_name + "\nNumber of vaccinated people: " + str(num_vaccinated)

        sql = "INSERT INTO stats VALUES (%s, %s, %s, %s)"
        val = (None, "leastVaccinatedCountry", str_val, datetime.now())
        cur.execute(sql, val)
        conn.commit()
    except:
        conn.rollback()
