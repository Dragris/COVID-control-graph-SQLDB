def delete_all(conn):
    cur = conn.cursor
    try:
        cur.execute("SET SQL_SAFE_UPDATES = 0")
        cur.execute("DELETE FROM person")
        cur.execute("DELETE FROM city")
        cur.execute("DELETE FROM country")
        cur.execute("DELETE FROM vaccine")
        cur.execute("DELETE FROM strain")
        conn.commit()
    except:
        conn.rollback()
        raise


def add_country(conn, name, population):
    cid = None
    cur = conn.cursor
    try:
        sql = "INSERT INTO city VALUES (%s, &s, %s)"
        val = (None, name, population)
        cur.execute(sql, val)
        cid = cur.lastrowid
    except:
        conn.rollback()
        raise
    conn.commit()
    return cid
