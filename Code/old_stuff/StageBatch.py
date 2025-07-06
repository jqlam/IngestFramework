# NECESSARY IMPORTS
import psycopg2
import ReadPokemonData
import datetime
import sys

# USE INGEST CONTROL FOR INPUTING DATA INTO THE DATABASE
    # CHECK FOR PREVIOUS ENTRY WITH MATCHING NAME
    # UPDATE IF MATCHING NAME
    # RECORD OF BATCH NUMBERS.

def ingestFile(filename):
    try:
        conn = psycopg2.connect(
            dbname = "pkm",
            user = "dbadmin",
            password = "tbl102",
            host = "localhost"
        )
        print("Connection Successful")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL database:", error)
    
    cursor = conn.cursor()

    records = ReadPokemonData.parseFile(filename, "0")

    psq = "insert into pocket.pokemon_raw(pokemon_name,color,type_id_1,type_id_2,region,egg_id_1,egg_id_2,pre_evo,batch_id)\nvalues"
    for record in records:
        psq += "\n"
        psq += "('" + record.name + "',"
        psq += "'" + record.color + "','"
        psq += record.type_id_1 + "',"
        if (len(record.type_id_2) == 0):
            psq += "null,'"
        else:
            psq += "'" + record.type_id_2 + "','"
        psq += record.region + "','"
        psq += record.egg_id_1 + "',"
        if (len(record.egg_id_2) == 0):
            psq += "null,"
        else:
            psq += "'" + record.egg_id_2 + "',"
        if (len(record.preevo) <= 1):
            psq += "null,"
        else:
            psq += "'" + record.preevo + "',"
        x = datetime.datetime.now()
        psq += x.strftime("%Y")
        psq += x.strftime("%m")
        psq += x.strftime("%d")
        psq += x.strftime("),")
        # psq += "20250000),"
    psq = psq[:-1]  #Remove last comma and new line in string
    psq += ";"

    print(psq)
    cursor.execute(psq)
    print("Execute complete")
    
    psq = "update pocket.control_table set stage_max_batch = " + x.strftime("%Y") + x.strftime("%m") + x.strftime("%d") + ";"
    cursor.execute(psq)

def main(filename):
    ingestFile(filename)

if __name__ == "__main__":
    main(sys.argv[1])