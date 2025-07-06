import psycopg2

try:
    conn = psycopg2.connect(
        dbname = "pkm",
        user = "dbadmin",
        password = "tbl102",
        host = "localhost"
    )
    cursor = conn.cursor()
    psq = "select * from pocket.pokemon"

    cursor.execute(psq)
    records = cursor.fetchall()
    for row in records:
        print("ID: ", row[0])
        print("Name: ", row[1])
        print("Color: ", row[2])
        print("Type 1: ", row[3])
        print("Type 2: ", row[4])
        print("Region: ", row[5])
        print("Egg 1: ", row[6])
        print("Egg 2: ", row[7])
        print("Pre-Evo: ", row[8])
        print("Creation Time:", row[9])
        print("Update Time: ",row[10], "\n")
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL database:", error)