if __name__ == '__main__':
    def doQuery(conn):
        cur = conn.cursor()

        cur.execute("SELECT * FROM person")

        for _ in cur.fetchall():
            print(_)


    print("Using mysqlclient (MySQLdb):")
    import MySQLdb

    myConnection = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database)
    doQuery(myConnection)
    myConnection.close()

    print("Using pymysql:")
    import pymysql

    myConnection = pymysql.connect(host=hostname, user=username, passwd=password, db=database)
    doQuery(myConnection)
    myConnection.close()