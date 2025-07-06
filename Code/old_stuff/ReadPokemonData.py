# This will parse a file for table usage.
import sys
from utilities.parseDelimitedString import parseDelimitedString

class Record:
    def __init__(self, name, color, type_id_1, region, egg_id_1, type_id_2 = "", egg_id_2 = "", preevo = ""):
        self.name = name
        self.color = color
        self.type_id_1 = type_id_1
        self.type_id_2 = type_id_2
        self.region = region
        self.egg_id_1 = egg_id_1
        self.egg_id_2 = egg_id_2
        self.preevo = preevo

class lookupRecord:
    def __init__(self, lookup_id, value):
        self.id = lookup_id
        self.value = value

# Opens and parses a given file.
def parseFile(fileName, lookup):
    f = open(fileName)
    lines = f.readlines()
    records = []
    if lookup == "0":
        for line in lines:
            data = parseDelimitedString(line)
            record = Record(data[0], data[1], data[2], data[4], data[5], data[3], data[6], data[7])
            records.append(record)
    else:
        for line in lines:
            data = parseDelimitedString(line)
            record = lookupRecord(data[0], data[1])
            records.append(record)
    f.close()
    return records

def main(fileName, lookup):
    if lookup == "0":
        records = parseFile(fileName, lookup)
        headers = ["NAME", "COLOR", "TYPE 1", "TYPE 2", "REGION", "EGG 1", "EGG 2", "PRE-EVOLUTION"]
        headerString = f"{headers[0]:<15} {headers[1]:<15} {headers[2]:<15} {headers[3]:<15} {headers[4]:<15} {headers[5]:<15} {headers[6]:<15} {headers[7]:<15}\n"
        print(headerString)
        for record in records:
            string = f"{record.name:<15} {record.color:<15} {record.type_id_1:<15} {record.type_id_2:<15} {record.region:<15} {record.egg_id_1:<15} {record.egg_id_2:<15} {record.preevo:<15}\n"
            print(string)
    else:
        records = parseFile(fileName)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
        