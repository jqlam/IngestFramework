# Check the control table for max stage and max ingest
# Pull from raw and edit data to put into pokemon using the reference tables

import psycopg2
import sys
import ReadPokemonData

def translateType(type_name):
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

def translateRegion(region):
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

def translateEgg(egg):
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

def ingestData ():
    try:
        conn = psycopg2.connect(
            dbname = "pkm",
            user = "dbadmin",
            password = "tbl102",
            host = "localhost"
        )
        cursor = conn.cursor()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL database:", error)
    
    psq = "select * from pocket.control_table;"
    cursor.execute(psq)
    batches = cursor.fetchone()
    max_stage = batches[1]
    max_ingest = batches[3]

    psq = f"select * from pocket.pokemon_raw pr where pr.batch_id > {max_ingest} and pr.batch_id < {max_stage};"
    cursor.execute(psq)

    records = cursor.fetchall()

    psq = "insert into pocket.pokemon(pokemon_name,color,type_id_1,type_id_2,region,egg_id_1,egg_id_2,pre_evo)\nvalues"
    for record in records:
        psq += "\n"
        psq += "('" + record[1] + "',"
        psq += "'" + record[2] + "',"
        psq += translateType(record[3]) + ","
        psq += translateType(record[4]) + ","
        psq += translateRegion(record[5]) + ","
        psq += translateRegion(record[6]) + ","
        psq += translateEgg(record[7]) + ","
        psq += translateEgg(record[8]) + ","
        psq += record[9] + "'),"
    psq = psq[:-2]  #Remove last comma and new line in string
    psq += ";"

    cursor.execute(psq)

    