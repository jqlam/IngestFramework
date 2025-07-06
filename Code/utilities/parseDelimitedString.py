# Parse line with given delimiter, defaults to a comma
def parseDelimitedString(string, delimiter = ","):
    return string.split(delimiter) # Returns a list of words split by the delimeter