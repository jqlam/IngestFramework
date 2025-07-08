import psycopg2
import datetime
import sys

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
        # lines = f.readlines()
        # for line in lines:
        #     print(line)
        batch = self._get_batch()
        try:
            cursor.copy_expert("copy pocket.pokemon_raw (pokemon_name, color, type_id_1, type_id_2, region, egg_id_1, egg_id_2, pre_evo) from STDIN with (format 'csv', header false)", f)
        except:
            print("Something went wrong with the SQL statement!")
            self._conn.rollback()
            #self._log("Error", "Failed attempt to ingest new data")
        lines = len(f.readlines())
        sql = (f"update pocket.pokemon_raw set batch_id = {batch} where batch_id = NULL;")
        cursor.execute(sql)
        self._update_control_table(batch)
        #self._log("log", f"{lines} added in batch {batch}")
        if commit:
            self._conn.commit()
        cursor.close()
        f.close()
        return batch
    
    def _log(self, log_level: str, log_message: str, invoker: str = "jqlam", commit: bool = True):
        cursor = self._conn.cursor()
        sql = (f"insert into pocket.log_table(invoker, log_level, log_message) values ('{invoker}','{log_level}','{log_message}');")
        cursor.execute(sql)
        if commit:
            self._conn.commit()
        cursor.close()

    def _get_batch(self):
        x = datetime.datetime.now()
        batch = x.strftime("%Y")
        batch += x.strftime("%m")
        batch += x.strftime("%d")
        return batch
    
    def _update_control_table(self, batch):
        column = "ingest_max_batch"
        cursor = self._conn.cursor()
        sql = (f"update pocket.control_table set {column} = {batch};")
        cursor.execute(sql)
        cursor.close()

def main(filename):
    print(filename)
    ingestTest = PokemonIngestor(filename)
    ingestTest.ingest_raw_data()

if __name__ == "__main__":
    main(sys.argv[1])