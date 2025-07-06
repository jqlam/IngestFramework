import logging
import psycopg2
import os
import inspect
import sys

logging.basicConfig(format='%(asctime)s:: %(levelname)s: %(message)s', level=logging.DEBUG)

class RawIngestor:
    def __init__(self, db_name, schema_name, input_file_path):
        try:
            # self._host = os.getenv("POSTGRES_SERVER_NAME")
            # self._user = os.getenv("POSTGRES_USER") #"cdradm"
            # self._password = os.getenv("POSTGRES_PWD")            
            #self._host = "dev-ciquestdb.postgres.database.usgovcloudapi.net"
            self._host = "localhost"
            self._user = "cdradm"
            self._password = "ChangeMe!"
            self._port = "5432"
            self._sslmode = "require"
            self._dbname = db_name
            self._schema_name = schema_name
            self._input_file_path = input_file_path
            self._base_dir = "/home/tlam/project/test/my-raw-ingest/dataset/"
            self._db_connection_open()
        except:
          pass


    def __del__(self):
        self._db_connection_close()



    def _db_connection_open(self):
        invoker = inspect.currentframe().f_code.co_name
        try:
            self._conn_str = "host={0} user={1} dbname={2} password={3} sslmode={4}". \
                format(self._host, self._user, self._dbname, self._password, self._sslmode)
            self._conn = psycopg2.connect(self._conn_str)
            logging.info(f"{invoker}:: SUCCESS: db connection established")

        except(Exception, psycopg2.Error) as ex:
            logging.error(f"{invoker}:: ERROR: unable to connect to db| {ex}")
            exit(1)



    def _db_connection_close(self):
        invoker = inspect.currentframe().f_code.co_name
        try:
            if self._conn:
                self._conn.close()
                self._conn = None
                logging.info (f"{invoker}:: SUCCESS: db connection closed")
        except:
            pass


    def _execute_sql(self, sql: str) -> list:
        invoker = inspect.currentframe().f_code.co_name
        try:
            rows = None
        
            cursor = self._conn.cursor()
            cursor.execute(sql)
        
            rows = cursor.fetchall()
            cursor.close()
            return rows
        
        except(Exception, psycopg2.Error) as ex:
            logging.error(f"{invoker}:: ERROR: unable to execute sql [{sql}]| [{ex}]")


    def _execute_ddl_dml(self, sql: str, commit: bool = False) -> str:
        invoker = inspect.currentframe().f_code.co_name
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)

            if commit:
                self._conn.commit()
            cursor.close()

            return str(cursor.rowcount)
        
        except(Exception, psycopg2.Error) as ex:
            logging.error(f"{invoker}:: ERROR: unable to execute ddl/dml [{sql}]| [{ex}]")


    def _execute_copy_expert(self, sql: str, file_handle: str, commit: bool = False) -> str:
        invoker = inspect.currentframe().f_code.co_name
        try:
            cursor = self._conn.cursor()
            cursor.copy_expert(sql, file_handle)
            
            if commit:
                self._conn.commit()
            cursor.close()

            return str(cursor.rowcount)
        
        except (Exception, psycopg2.Error) as ex:
            logging.error(f"{invoker}:: ERROR: unable to execute copy_expert [{sql}]| [{ex}]")


    def _get_query_type(self, sql: str):
        DDL_DML_CMD = ("insert ", "update ", "delete ", "grant ", "create ")
        COPY_CMD = "copy"

        derived_sql = sql.lower()
        query_type = ""

        if "copy" in derived_sql:
            query_type = "COPY"
        else:
            for cmd in DDL_DML_CMD:
                if cmd in derived_sql:
                    query_type = "DDL_DML"
                    break
            else:
                query_type = "SELECT"
        return query_type


#------------------------------------------------------------------------------
    def _get_artifact_file_path(self, schema_name):
        base_dir = self._base_dir
        file_name = schema_name + "_artifacts.sql"
        file_path = base_dir + file_name
        return file_path
    

    # copy schema.table xxx; --directory
    #def _get_data_file_path(self, schema_name, raw_sql):
    def _get_sql_directory(self, schema_name, raw_sql):
        invoker = inspect.currentframe().f_code.co_name
        (sql, directory) = raw_sql.split("--")
        directory = f"{schema_name}/{directory.strip()}"
        logging.info(f"{invoker}:: directory={directory}, sql={sql}")
        return sql, directory


    def _copy_data(self, schema_name, raw_sql):
        invoker = inspect.currentframe().f_code.co_name
        logging.info(f"{invoker}:: BEGIN")
        (sql, data_directory) = self._get_sql_directory(schema_name, raw_sql)

        try:
            storage_connection_string = self._storage_connection_string
            container_name = self._storage_container_name
            logging.info(f"{invoker}:: container_name={container_name}")

            # file_name = "dsc_normalized_data.txt"
            # file_path = self._get_data_file_path(schema_name, file_name)
            # logging.info(f"{invoker}:: file_path={file_path}")

            # get a list of files in data_directory
            blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
            container_client = blob_service_client.get_container_client(container=container_name)
            blob_list = container_client.list_blobs(name_starts_with=data_directory)   
            
            # iterate through directory and load each file
            for blob in blob_list:
                file_path = f"{blob.name}"
                logging.info(f"{invoker}:: {file_path}")
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
                file_handle = blob_client.download_blob(max_concurrency=1, encoding="UTF-8")
                row_count = self._execute_copy_expert(sql, file_handle, True)
                logging.info(f"{invoker}:: data_file={file_path}, row_count={row_count}")

            logging.info(f"{invoker}:: END")

            return row_count
        except Exception as ex:
            logging.error(f"{invoker}:: ERROR: unable to copy data to db [{sql}]| [{ex}]")
    


    # only get the commands between the BEGIN and END <file_marker>
    def _get_commands(self, ds_file_path, file_marker):
        invoker = inspect.currentframe().f_code.co_name

        cmd = ""
        commands = []
        read_line = False

        logging.debug(f"command_file={ds_file_path}, file_marker={file_marker}")

        begin_marker = "--BEGIN " + file_marker
        end_marker = "--END " + file_marker
        end_marker_prev = "--END "

        try: 
            with open(ds_file_path, "r") as infile:
                for line in infile:
                    if begin_marker in line:
                        read_line = True
                        continue
                    elif end_marker in line:
                        read_line = False
                        break
                    elif end_marker_prev in line:
                            continue
                    if read_line:
                        cmd += line
                        if ";" in line:
                            commands.append(cmd.strip())        
                            cmd = ""

            return commands
        except Exception as ex:
            logging.error(f"{invoker}:: ERROR: unable to open file: {ds_file_path}| [{ex}]")


#==============================================================================
    def _create_schema(self, schema_name):
        invoker = inspect.currentframe().f_code.co_name

        if not self._conn:
            raise ValueError("no db connection to continue")
            #logging.error("no db connection")
            #sys.exit("no db connection")

        SQLS = [
            'drop schema if exists {schema} cascade',
            'create schema {schema} authorization cdr_admin',
            'grant usage on schema {schema} to osint_read',
            'grant select on all tables in schema {schema} to osint_read']
        msg = f"schema {schema_name}"
        logging.info (f"{invoker}:: BEGIN {msg}")

        try:
            for sql in SQLS:
                sql = sql.format(schema=schema_name)
                row_count = self._execute_ddl_dml(sql, True)
            logging.info (f"{invoker}:: END {msg}")
        
        except(Exception) as ex:
            logging.error(f"{invoker}:: ERROR: unable to execute sql [{sql}]| {ex}")


#------------------------------------------------------------------------------  
    def _create_schema_artifacts(self, schema_name):
        invoker = inspect.currentframe().f_code.co_name

        # commands in file are delinated by file_marker
        file_marker = invoker[1:]

        command_file_path = self._get_artifact_file_path(schema_name)

        msg = f"(command file={command_file_path})"
        logging.info(f"{invoker}:: BEGIN {msg}")

        sqls = self._get_commands(command_file_path, file_marker)

        try:
            for sql in sqls:
                row_count = self._execute_ddl_dml(sql, True)
                logging.info(f"{invoker}:: sql: {sql}")
            
            logging.info (f"{invoker}:: END {msg}")

        except(Exception) as ex:
            msg = f"ERROR: unable to execute sql {sql}| {ex}"
            logging.error(f"{invoker}:: {msg}")


#------------------------------------------------------------------------------
    def _load_raw_data(self, schema_name):
        invoker = inspect.currentframe().f_code.co_name  
        
        # commands in file are delinated by file_marker
        file_marker = invoker[1:]

        command_file_path = self._get_artifact_file_path(schema_name)

        logging.info(f"{invoker}:: BEGIN")

        sqls = self._get_commands(command_file_path, file_marker)
    
        for sql in sqls:
            query_type = self._get_query_type(sql)
            logging.info(f"{invoker}:: query_type={query_type}| sql={sql}")

            match query_type:
                case 'DDL_DML': 
                    row_count = self._execute_ddl_dml(sql, True)
                case 'COPY': 
                    row_count = self._copy_data(schema_name, sql)
                    #row_count = self._execute_copy_expert(sql, infile, True)
                case 'SELECT':
                    rows = self._execute_sql(sql)
                    row_count = len(rows)
                case _:
                    row_count = 0

        logging.info (f"{invoker}:: END")


#------------------------------------------------------------------------------
    def ingest_dataset(self) -> bool:
        invoker = inspect.currentframe().f_code.co_name
        try:
            schema_name = self._schema_name
            
            self._create_schema(schema_name)
            self._create_schema_artifacts(schema_name)
            self._load_raw_data(schema_name)
            # create udo shell
            # insert data into udo_staging
            # merge to udo


        except psycopg2.Error as db_ex:
            logging.error(f"{invoker}:: ERROR: db error {db_ex}")
            return False
        except Exception as ex:
            logging.error(f"{invoker}:: ERROR: {ex}")
            return False
        finally:
            self._db_connection_close()
        return True
        

#------------------------------------------------------------------------------
    def get_user_info(self, userid: str) -> list:
        invoker = inspect.currentframe().f_code.co_name
        try:
            sql_template = "SELECT first_name, last_name, email, username, modify_ts FROM test_tl.person where id={id}" 
            sql = sql_template.format(id = userid)
            rows = self._execute_sql(sql)

            for row in rows:
                response = "Name: {name}; email: {email}; username: {username}". \
                    format(name = row[0]+ ' '+row[1], email = row[2], username = row[3] + "!")
            return response
        except:
            logging.error(f"{invoker}:: ERROR: unable to query db")