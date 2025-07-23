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
        print("Deleting object")
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
                self._log("connection", "Closed connection")
                self._conn.close()
                self._conn = None
        except:
            pass

    def ingest_raw_data(self, commit: bool = True):
        cursor = self._conn.cursor()
        f = open(self._file_name, 'r')
        batch = self._get_batch()
        try:
            cursor.copy_expert("copy pocket.pokemon_raw (pokemon_name, color, type_id_1, type_id_2, region, egg_id_1, egg_id_2, pre_evo) from STDIN with (format 'csv', header false)", f)
        except:
            print("Something went wrong with the SQL statement!")
            self._log("error", "Failed attempt to ingest new data")
        lines = self._get_lines
        sql = (f"update pocket.pokemon_raw set batch_id = {batch} where batch_id is NULL;")
        cursor.execute(sql)
        self._update_control_table(batch, "stage_max_batch")
        self._log("log", str(f"{lines} added in batch {batch}"))
        if commit:
            self._conn.commit()
        cursor.close()
        f.close()
        return batch

    def _log(self, log_level: str, log_message: str, invoker: str = "jqlam", commit: bool = True):
        cursor = self._conn.cursor()
        psq = f"insert into pocket.log_table(invoker, log_level, log_message) values ('{invoker}','{log_level}','{log_message}');"
        cursor.execute(psq)
        if commit:
            self._conn.commit()
        cursor.close()

    def _get_batch(self):
        x = datetime.datetime.now()
        batch = x.strftime("%Y")
        batch += x.strftime("%m")
        batch += x.strftime("%d")
        return batch
    
    def _update_control_table(self, batch, column):
        cursor = self._conn.cursor()
        psq = (f"update pocket.control_table set {column} = {batch};")
        cursor.execute(psq)
        cursor.close()
    
    def _has_data(self) -> bool:
        cursor = self._conn.cursor()
        sql = "select * from pocket.control_table;"
        cursor.execute(sql)
        entries = cursor.fetchone()
        stage_max = int(entries[1])
        ingest_max = int(entries[3])
        return stage_max > ingest_max
    
    def _translateType(self, type_name):
        if (type_name == "Bug"):
            return "1"
        elif (type_name == "Dark"):
            return "2"
        elif (type_name == "Dragon"):
            return "3"
        elif (type_name == "Electric"):
            return "4"
        elif (type_name == "Fairy"):
            return "5"
        elif (type_name == "Fighting"):
            return "6"
        elif (type_name == "Fire"):
            return "7"
        elif (type_name == "Flying"):
            return "8"
        elif (type_name == "Ghost"):
            return "9"
        elif (type_name == "Grass"):
            return "10"
        elif (type_name == "Ground"):
            return "11"
        elif (type_name == "Ice"):
            return "12"
        elif (type_name == "Normal"):
            return "13"
        elif (type_name == "Poison"):
            return "14"
        elif (type_name == "Psychic"):
            return "15"
        elif (type_name == "Rock"):
            return "16"
        elif (type_name == "Steel"):
            return "17"
        elif (type_name == "Water"):
            return "18"
        else:
            return "null"
        
    def _translateRegion(self, region):
        if (region == "Kanto"):
            return "1"
        elif (region == "Johto"):
            return "2"
        elif (region == "Hoenn"):
            return "3"
        elif (region == "Sinnoh"):
            return "4"
        elif (region == "Unova"):
            return "5"
        elif (region == "Kalos"):
            return "6"
        elif (region == "Alola"):
            return "7"
        elif (region == "Galar"):
            return "8"
        elif (region == "Paldea"):
            return "9"
        elif (region == "Hisui"):
            return "10"
        else:
            return "null"

    def _translateEgg(self, egg):
        if (egg == "Amorphous"):
            return "1"
        elif (egg == "Bug"):
            return "2"
        elif (egg == "Dragon"):
            return "3"
        elif (egg == "Fairy"):
            return "4"
        elif (egg == "Field"):
            return "5"
        elif (egg == "Flying"):
            return "6"
        elif (egg == "Grass"):
            return "7"
        elif (egg == "Human-Like"):
            return "8"
        elif (egg == "Mineral"):
            return "9"
        elif (egg == "Monster"):
            return "10"
        elif (egg == "Water 1"):
            return "11"
        elif (egg == "Water 2"):
            return "12"
        elif (egg == "Water 3"):
            return "13"
        elif (egg == "Ditto"):
            return "14"
        elif (egg == "Undiscovered"):
            return "15"
        else:
            return "null"
        
    def _get_max_ingest_batch(self) -> int:
        cursor = self._conn.cursor()
        sql = "select * from pocket.control_table;"
        cursor.execute(sql)
        entries = cursor.fetchone()
        ingest_max = int(entries[3])
        return ingest_max

    def ingest_data(self, commit: bool = True):
        if not self._has_data:
            return
        cursor = self._conn.cursor()
        max_ingest_batch = self._get_max_ingest_batch()
        sql = f"select * from pocket.pokemon_raw where batch_id > {max_ingest_batch};"
        cursor.execute(sql)
        new_data = cursor.fetchall()
        counter = 0
        max_batch = 0
        for entry in new_data:
            name = entry[1]
            color = entry[2]
            type_1 = self._translateType(entry[3])
            type_2 = self._translateType(entry[4])
            region = self._translateRegion(entry[5])
            egg_1 = self._translateEgg(entry[6])
            egg_2 = self._translateEgg(entry[7])
            pre_evo = entry[8]
            if max_batch < int(entry[11]):
                max_batch = int(entry[11])
            sql = f"insert into pocket.pokemon(pokemon_name, color, type_id_1, type_id_2, region, egg_id_1, egg_id_2, pre_evo) \
                values ('{name}', '{color}', {type_1}, {type_2}, {region}, {egg_1}, {egg_2}, '{pre_evo}');"
            cursor.execute(sql)
            counter += 1
        self._log("log", f"{counter} lines ingested into pocket.pokemon")
        self._update_control_table(max_batch, "ingest_max_batch")
        if (commit):
            self._conn.commit()

def main(filename):
    print(filename)
    ingestTest = PokemonIngestor(filename)
    ingestTest.ingest_data()
    del ingestTest

if __name__ == "__main__":
    main(sys.argv[1])