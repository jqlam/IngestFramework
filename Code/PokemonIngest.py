import psycopg2
import datetime

class PokemonIngestor:
    def __init__(self, file_name):
        try:
            self._file_name = file_name
            self._open_db_connection()
        except:
            pass
    
    def __del__(self):
        self._close_db_connection()
    
    def _open_db_connection(self):
        try:
            self._conn = psycopg2.connect(
                dbname = "pkm",
                user = "dbadmin",
                password = "tbl102",
                host = "localhost"
            )
            self._log("connection", "Opened Connection")
        except:
            self._log("error", "Failed Connection")
            exit(1)
    
    def _close_db_connection(self):
        try:
            if self._conn:
                self._conn.close()
                self._conn = None
                self._log("connection", "Closed connection")
        except:
            pass

    def ingest_raw_data(self, commit: bool = True):
        cursor = self._conn.cursor()
        f = open(self._file_name, 'r')
        cursor.copy_from(f, 'pocket.pokemon_raw',sep=',')
        self._update_control_table()
        if commit:
            self._conn.commit()
        cursor.close()
        f.close()
    
    def _log(self, log_level: str, log_message: str, invoker: str = "jqlam", commit: bool = True):
        cursor = self._conn.cursor()
        sql = (f"insert into pocket.log_table(invoker, log_level, log_message) values (\"{invoker}\",\"{log_level}\",\"{log_message}\");")
        cursor.execute(sql)
        if commit:
            self._conn.commit()
        cursor.close()

    def _get_batch():
        x = datetime.datetime.now()
        batch = x.strftime("%Y")
        batch += x.strftime("%m")
        batch += x.strftime("%d")
        return batch
    
    def _update_control_table(self, batch, ingest: bool = True):
        if (ingest):
            column = "ingest_max_batch"
        else:
            column = "stage_max_batch"
        cursor = self._conn.cursor()
        sql = (f"update pocket.control_table set {column} = {batch};")
        cursor.execute(sql)
        cursor.close()
        