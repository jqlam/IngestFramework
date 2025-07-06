import psycopg2
import sys
import ReadPokemonData

def call_function(function_name, var1):
    try:
        conn = psycopg2.connect(
            dbname = "pkm",
            user = "dbadmin",
            password = "tbl102",
            host = "localhost"
        )
        cursor = conn.cursor()

        psq = "select * from pocket." + str(function_name) + "(" + str(var1) + ");"

        cursor.execute(psq)
        records = cursor.fetchall()
        data = ""
        for row in records:
            data += "ID: " + str(row[0]) + "\n"
            data += "Name: " + str(row[1]) + "\n"
            data += "Color: " + str(row[2]) + "\n"
            data += "Type 1: " + str(row[3]) + "\n"
            data += "Type 2: " + str(row[4]) + "\n"
            data += "Region: " + str(row[5]) + "\n"
            data += "Egg 1: " + str(row[6]) + "\n"
            data += "Egg 2: " + str(row[7]) + "\n"
            data += "Pre-Evo: " + str(row[8]) + "\n"
            data += "Creation Time:" + str(row[9]) + "\n"
            data += "Update Time: " + str(row[10]) + "\n\n"
        return data  
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL database:", error)

def main(function_name, var):
    output = call_function(function_name, var)
    print(output)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])