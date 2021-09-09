import psycopg2
from config import config

def insert_page(page_name):
    """ insert a new page into the page table """
    sql = """INSERT INTO Page(name)
             VALUES(%s);"""
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (page_name,))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_influenced(page_name, influence):
    """ insert a new page into the page table """
    sql = """INSERT INTO influenced (name, influenced)
             VALUES(%s, %s);"""
    conn = None
    #insert_page(page_name)
    #insert_page(influence)
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (page_name, influence))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()            

    return

def insert_page_list(page_list):
    """ insert multiple pages into the page table  """
    sql = "INSERT INTO Page(name) VALUES(%s)"
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.executemany(sql, (page_list,))
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def check_page_exist(page_name):
    """ query data from the a table """
    sql = "SELECT name FROM Page WHERE name=%s"
    conn = None
    check = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (page_name,))
        check = cur.rowcount > 0

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return check

def check_influenced_exist(page_name, influence):
    """ query data from the a table """
    conn = None
    check = None
    sql = "SELECT name FROM influenced WHERE name=%s and influenced=%s"
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (page_name, influence))
        check = cur.rowcount > 0

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return check    


def get_influenced():
    """ query data from the a table """
    conn = None
    data = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT * FROM Influenced order by name")
        data = cur.fetchall()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return data


def get_nodes_with_degree():
    """ query data from the a table """
    conn = None
    data = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("select page.name, count(influenced) from page left join influenced on page.name = influenced.name  group by page.name order by name")
        data = cur.fetchall()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return data