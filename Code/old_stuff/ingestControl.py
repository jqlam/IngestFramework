import psycopg2
import ReadPokemonData

def ingestFile(filename):
    try:
        conn = psycopg2.connect(
            dbname = "pkm",
            user = "dbadmin",
            password = "tbl102",
            host = "localhost"
        )
        cursor = conn.cursor()

        records = ReadPokemonData.parseFile(filename, 0)

        psq = "insert into pocket.pokemon(pokemon_name,color,type_id_1,type_id_2,region,egg_id_1,egg_id_2,pre_evo)\nvalues"
        for record in records:
            psq += "\n"
            psq += "('" + record.name + "',"
            psq += "'" + record.color + "',"
            psq += record.type_id_1 + ","
            if (len(record.type_id_2) == 0):
                psq += "null,"
            else:
                psq += record.type_id_2 + ","
            psq += record.region + ","
            psq += record.egg_id_1 + ","
            if (len(record.egg_id_2) == 0):
                psq += "null,"
            else:
                psq += record.egg_id_2 + ","
            if (len(record.preevo) == 0):
                psq += "null),"
            else:
                psq += "'" + record.preevo + "'),"
        psq = psq[:-2]  #Remove last comma and new line in string
        psq += ";"

        cursor.execute(psq)

        conn.close()
        
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL database:", error)