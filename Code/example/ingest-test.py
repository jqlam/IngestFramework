import logging
from processor.raw_ingestor import RawIngestor;
import sys

dbname = "osint"
schema_name = "dsc"
input_file_path = "/home/tlam/project/test/my-raw-ingest/dsc_normalized_data.txt"
# sqlCmd = [
#     #'create schema dsb authorization cdr_admin',
#     #'grant usage on schema dstest to osint_read',
#     #'grant select on all tables in schema dsb to osint_read'
#     'select * from osint.dsa.raw_data'
# ]

try:
    dataset_c = RawIngestor(dbname, schema_name, input_file_path)
    #dataset_c.ingest_dataset()
except (Exception) as ex:
    sys.exit(1)
else:
    dataset_c.ingest_dataset()


#schema_name = 'test_tl'
#x = RawIngestor(dbname, schema_name, input_file_path)
#result = x.get_user_info("1")
#print (result)
#myDb.open_connection()

# sql = "select * from osint.dsa.normalized_data"
# rows = dsb_ingest.execute_sql(sql)
# print(rows)

# for sql in sqlCmd:
#     dsb_ingest.execute_sql(sql)
#     print("SUCCESS: " + sql)

