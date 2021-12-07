def delete_all(conn):
    cur = conn.cursor()
    try:
        cur.execute("SET SQL_SAFE_UPDATES = 0")

        cur.execute("DROP TABLE person")
        cur.execute(
            "CREATE TABLE person (`idperson` int NOT NULL AUTO_INCREMENT, `first_name` varchar(45) NOT NULL, `last_name` varchar(45) NOT NULL, `sex` char(1) NOT NULL, `dob` datetime NOT NULL, PRIMARY KEY (`idperson`), UNIQUE KEY `idperson_UNIQUE` (`idperson`))"
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
        conn.rollback()
        raise
    conn.commit()
    return cid
