import sqlite3
import os
from contextlib import contextmanager
import logging


@contextmanager
def create_connection(db_file="./data/database.db"):
    conn = sqlite3.connect(db_file)
    try:
        yield conn
    except Exception as e:
        logging.exception(e)
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()


def __execute_write(query, *args):
    with create_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, args)
        conn.commit()
        cur.close()


def __execute_fetchone(query, *args):
    with create_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, args)
        res = cur.fetchone()
        cur.close()
    return res

def __execute_fetchall(query, *args):
    with create_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, args)
        res = cur.fetchall()
        cur.close()
    return res


def __create_table(create_table_sql):
    with create_connection() as conn:
        cur = conn.cursor()
        cur.execute(create_table_sql)
        cur.close()


def init(db_file="./database.db"):
    sql_create_page_table = """CREATE TABLE Page (
        name text PRIMARY KEY
        );"""

    sql_create_influenced_table = """CREATE TABLE "Influence" (
        "source"	text,
        "target"	text,
        FOREIGN KEY("target") REFERENCES "Page"("name"),
        FOREIGN KEY("source") REFERENCES "Page"("name"),
        CONSTRAINT "PK_Influenced" PRIMARY KEY("source","target")
        );"""

    if os.path.exists(db_file):
        os.remove(db_file)

    __create_table(sql_create_page_table)
    __create_table(sql_create_influenced_table)


def insert_page(page_name):
    """insert a new page into the page table"""
    sql = """INSERT INTO Page(name)
             VALUES(?);"""
    __execute_write(sql, page_name)


def insert_influenced(page_name, influence):
    """insert a new page into the page table"""
    sql = """INSERT INTO Influence(source, target)
             VALUES(?, ?);"""
    __execute_write(sql, page_name, influence)


def page_exists(page_name):
    """query data from the a table"""
    sql = "SELECT EXISTS (SELECT name FROM Page WHERE name=(?))"
    return __execute_fetchone(sql, page_name)[0]


def influenced_exist(page_name, influence):
    """query data from the a table"""
    sql = (
        "SELECT EXISTS (SELECT name FROM Influence WHERE source=(?) and target=(?))"
    )
    return __execute_fetchone(sql, page_name, influence)[0]


def get_nodes_json():
    """query data from the a table"""
    sql = "select page.name as id, count(source) as val from page left join influence on page.name = influence.source group by page.name order by val desc"
    with create_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
    return r


def get_edges_json():
    """query data from the a table"""
    sql = "SELECT * from influence"
    with create_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
    return r