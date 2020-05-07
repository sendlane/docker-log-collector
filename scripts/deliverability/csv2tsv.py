import csv
import sys
import json
from io import StringIO

for line in sys.stdin:
    machine, data = line.strip().split("\t")
    data = data[2:-1]
    fh = StringIO(data)
    reader = csv.reader(fh)
    for record in reader:
        print("\t".join(record))

