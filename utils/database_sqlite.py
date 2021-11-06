import sqlite3
import os
from contextlib import contextmanager
import utils.database


class database(utils.database.database):
    def __init__(self, db_file="../data/database.db"):
        self.db_file = db_file

    @contextmanager
    def __create_connection(self):
        conn = sqlite3.connect(self.db_file)
        try:
            yield conn
        except sqlite3.Error as e:
            sqlite3.Error(e)
            if conn is not None:
                conn.rollback()
        finally:
            if conn is not None:
                conn.close()

    def __execute_write(self, query, *args):
        with self.__create_connection() as conn:
            try:
                cur = conn.cursor()
                cur.execute(query, args)
                conn.commit()
            except sqlite3.Error as e:
                raise sqlite3.Error(e)
            finally:
                cur.close()

    def __execute_fetchone(self, query, *args):
        res = None
        with self.__create_connection() as conn:
            try:
                cur = conn.cursor()
                cur.execute(query, args)
                res = cur.fetchone()
            except sqlite3.Error as e:
                raise sqlite3.Error(e)
            finally:
                cur.close()
        return res

    def __execute_fetchall(self, query, *args):
        with self.__create_connection() as conn:
            try:
                cur = conn.cursor()
                cur.execute(query, args)
                res = cur.fetchall()
            except sqlite3.Error as e:
                raise sqlite3.Error(e)
            finally:
                cur.close()
        return res

    def __create_table(self, create_table_sql):
        with self.__create_connection() as conn:
            try:
                cur = conn.cursor()
                cur.execute(create_table_sql)
            except sqlite3.Error as e:
                raise sqlite3.Error(e)
            finally:
                cur.close()

    def create_database(self):
        sql_create_page_table = """CREATE TABLE Page (
            id text PRIMARY KEY
            );"""

        sql_create_influenced_table = """CREATE TABLE "Influence" (
            "source"	text,
            "target"	text,
            FOREIGN KEY("target") REFERENCES "Page"("id"),
            FOREIGN KEY("source") REFERENCES "Page"("id"),
            CONSTRAINT "PK_Influenced" PRIMARY KEY("source","target")
            );"""

        if os.path.exists(self.db_file):
            os.remove(self.db_file)

        try:
            self.__create_table(sql_create_page_table)
            self.__create_table(sql_create_influenced_table)

            # Enable WAL
            with self.__create_connection() as conn:
                conn.execute("pragma journal_mode=wal")
        except sqlite3.Error as e:
            raise sqlite3.Error(e)

    def insert_page(self, page_name):
        """insert a new page into the page table"""
        sql = """INSERT INTO Page(id)
                VALUES(?);"""
        try:
            self.__execute_write(sql, page_name)
        except sqlite3.Error as e:
            raise sqlite3.Error(e)

    def insert_influenced(self, page_name, influence):
        """insert a new page into the page table"""
        sql = """INSERT INTO Influence(source, target)
                VALUES(?, ?);"""
        try:
            self.__execute_write(sql, page_name, influence)
        except sqlite3.Error as e:
            raise sqlite3.Error(e)

    def page_exists(self, page_name):
        """query data from the a table"""
        sql = "SELECT EXISTS (SELECT id FROM Page WHERE id=(?))"
        try:
            return self.__execute_fetchone(sql, page_name)[0]
        except sqlite3.Error as e:
            raise sqlite3.Error(e)

    def influenced_exist(self, page_name, influence):
        """query data from the a table"""
        sql = "SELECT EXISTS (SELECT source FROM Influence WHERE source=(?) and target=(?))"
        try:
            return self.__execute_fetchone(sql, page_name, influence)[0]
        except sqlite3.Error as e:
            raise sqlite3.Error(e)

    def get_nodes(self):
        """query data from the a table"""
        sql = "select page.id, count(source) as val from page left join influence on page.id = influence.source group by page.id order by val desc"
        try:
            return self.__execute_fetchall(sql)
        except sqlite3.Error as e:
            raise sqlite3.Error(e)

    def get_edges(self):
        """query data from the a table"""
        sql = "SELECT * from influence"
        try:
            return self.__execute_fetchall(sql)
        except sqlite3.Error as e:
            raise sqlite3.Error(e)
