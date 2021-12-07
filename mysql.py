def delete_all(conn):
    cur = conn.cursor
    try:
        cur.execute("DELETE * from person")
        cur.execute("DELETE * from city")
        cur.execute("DELETE * from country")
        cur.execute("DELETE * from vaccine")
        cur.execute("DELETE * from strain")
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
