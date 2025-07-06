raw_sql = "copy dsc.normalized_data(id, first_name, last_name, email, user_name) from stdin with csv header delimiter as ','; -- normalized"

# location = raw_sql.split("--")
# print (location[0])
# print (location[1])

# invoker = "_load_raw_data"
# file_marker = invoker[1:]

# print(file_marker)

# def get_sql_dir(schema_name, raw_sql): 
#     (sql, dir) = raw_sql.split("--")
#     dir = f"{schema_name}/{dir.strip()}"
#     return sql, dir
 
# # Driver code to test above method 
# sql, dir = get_sql_dir("dsc", raw_sql) # Assign returned tuple 
# print(sql) 
# print(f"dir={dir}") 

def get_payload(payload:dict):
    print(payload['ingest_type'])
    print(payload['schema_name'])

payload = {"ingest_type": "raw", "schema_name": "bitmax"}
get_payload(payload)



