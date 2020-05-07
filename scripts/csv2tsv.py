from io import StringIO
import csv
import sys

for line in sys.stdin:
    machine, data = line.strip().split("\t")
    fh = StringIO(data[2:-1])
    reader = csv.reader(fh)
    for record in reader:
        print("\t".join(record))
