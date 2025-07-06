import logging
import psycopg2
import psycopg2.pool
import os

class DbOpPool:
    def __init__(self, dbname):
        try:
            self._host = os.getenv("POSTGRES_SERVER_NAME")
            self._user = os.getenv("POSTGRES_USER") #"cdradm"
            self._password = os.getenv("POSTGRES_PWD")
            #self._host = "cidbtl.postgres.database.usgovcloudapi.net"
            #self._user = "cdradm"
            #self._password = "ChangeMe!"
            self._port = "5432"
            #self._sslmode = "require"
            self._conn_min = 1
            self._conn_max = 5
            self._dbname = dbname
            #print ("SUCCESS: initialize")
            logging.info("SUCCESS: DBOpPool intialize")
            self._create_connection_pool()
        except:    
            pass


    def __del__(self):
        self._close_connection_pool()


    def _create_connection_pool(self):
        try:
            self._pool = psycopg2.pool.SimpleConnectionPool(
                self._conn_min, self._conn_max,
                host=self._host,
                user=self._user,
                password=self._password,
                port=self._port,
                database=self._dbname
            )
            logging.info("SUCCESS: db connection pool created")
        except(Exception, psycopg2.Error) as error:
            logging.error("ERROR: unable to create connection pool ", error)


    def _get_connection(self):
        try:
            self._conn = self._pool.getconn()
            logging.info("SUCCESS: db connection gotten")
        except (Exception, psycopg2.Error) as error:
            logging.error("ERROR: unable to get connection ", error)


    def get_connection(self):
        self._get_connection()


    def _release_connection(self):
        try:
            self._pool.putconn(self._conn)
            logging.info("SUCCESS: db connection released")
        except:
            pass

    def release_connection(self):
        self._release_connection()


    def _close_connection_pool(self):
        if self._pool:
            self._pool.closeall
        logging.info("SUCCESS: db connection pool closed")
    

    def execute_sql(self, sql: str) -> list:
        try:
            rows = None
            
            self._get_connection()

            if (self._conn):
                cursor = self._conn.cursor()
                cursor.execute(sql)
            
                rows = cursor.fetchall()
                cursor.close()
                self._release_connection()
                return rows
            
        except(Exception, psycopg2.Error) as error:
            logging.error("ERROR: unable to execute ddl ", error)


    def execute_ddl_dml(self, sql: str, commit: bool = False) -> str:
        try:
            self._get_connection()

            cursor = self._conn.cursor()
            cursor.execute(sql)
            self._conn.commit()

            cursor.close()
            self._release_connection()
            return str(cursor.rowcount)
        except(Exception, psycopg2.Error) as error:
            logging.error("ERROR: unable to execute ddl/dml ", error)


    def execute_copy_expert(self, sql: str, file_handle: str, commit: bool = False) -> str:
        try:
            self._get_connection()

            cursor = self._conn.cursor()
            cursor.copy_expert(sql, file_handle)
            self._conn.commit()
            cursor.close()
            self._release_connection()
            return str(cursor.rowcount)
        except (Exception, psycopg2.Error) as error:
            logging.error("ERROR: unable to execute copy_expert ", error)
