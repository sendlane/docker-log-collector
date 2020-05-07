import argparse
import gzip
import shutil
import re
import time
import os
import sys
import pprint
import pid
from pid.decorator import pidfile

from os import walk
from datetime import date, datetime, timedelta

parser = argparse.ArgumentParser( description='Run stats for a given log interval and date')
parser.add_argument('-i', '--interval', default='daily', dest="interval", help="granularity [hourly or daily]")
args = parser.parse_args()
lockfile = f"archive-{args.interval}"

def gzip_file(record):
    source_file = record['filepath']
    with open(source_file, 'rb') as f_in, gzip.open(source_file + '.gz', 'wb', compresslevel=9) as f_out:
        shutil.copyfileobj(f_in, f_out)
        os.remove(source_file)
        record['is_compressed'] = True
        record['filepath'] = source_file + '.gz'
        record['filename'] = record['filename'] + '.gz'

def archive_file(record):
    source_file = record['filepath']
    prefix = record['prefix']
    year = record['year']
    month = record['month']
    day = record['day']
    hour = record['hour']
    is_compressed = record['is_compressed']
    filename = record['filename']

    if not(is_compressed):
        raise Exception(f"{source_file} got into archive call without being compressed")

    archive_path = f"/logs/archive/logs/{args.interval}/{prefix}/{year}/{month}/{day}"

    if os.path.isdir(archive_path) is not True:
        print(f"Making directory {archive_path}")
        os.makedirs(archive_path, 0o755)

    archive_filepath = f"{archive_path}/{filename}"
    print(f"Old path {source_file}, new path {archive_filepath}")
    os.rename(source_file,archive_filepath)

@pidfile(pidname=lockfile)
def main():

    print("Running....")    
    base = "/logs"
    base_plus_interval = base + '/' + args.interval 
    
    print("Processing the following directory")
    print(base_plus_interval)
    
    f = []
    current_time = datetime.now()
    min_time = None
    if args.interval == 'hourly':
        min_time = (current_time - timedelta(hours=1)).replace(minute=0,second=0,microsecond=0)
    elif args.interval == 'daily':
        min_time = (current_time - timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
    else:
        raise Exception("Interval is required")
    
    for (dirpath, dirnames, filenames) in walk(base_plus_interval):
        for path in filenames:
            fullpath = f"{dirpath}/{path}"
            regex = r'(.+?)-(\d{4})(\d{2})(\d{2})(\-)?(\d+)?.log(.gz)?$'
            print(f"Regex: {regex}")
            match = re.match(regex, path)
        
            if match is not None:
                prefix = match.group(1)
                year = match.group(2)
                month = match.group(3)
                day = match.group(4)
                hour = match.group(6)
                is_compressed = True if match.group(7) == '.gz' else False
    
                file_time = datetime(year=int(year),month=int(month),day=int(day),hour=int(hour) if hour else 0, minute=0, second=0)
                delta_seconds = file_time - min_time
    
                modification_time = time.time() - os.path.getmtime(fullpath)
    
                if (file_time <= min_time and modification_time >= 60 * 2) or is_compressed is True:
                    print(f"{path} can be archived since its time is {file_time} vs {min_time}")
                else:
                    print(f"{path} is not ready yet, it will be ready in {delta_seconds}")
                    continue
    
                info = dict(sort_field=f"{year}{month}{day}{hour}{prefix}",prefix=prefix, year=year, month=month, day=day, hour=hour, is_compressed=is_compressed, filepath=fullpath, filename=path)
                f.append(info)
    
    sorted_files = sorted(f, key=lambda record: record['sort_field'])
    
    pprint.pprint(sorted_files)
    
    for record in sorted_files:
        fullpath = record['filepath']
        print(f"Looking at file : {fullpath}")
        if not(record['is_compressed']):
            print(f"Compressing {fullpath}")
            gzip_file(record)
            print(f"compression done for {fullpath}")
    
        print(f"archiving : {fullpath}")
        archive_file(record)

if __name__ == '__main__':

    try:
        main()
    except pid.base.PidFileError as exc:
        print("Already Running...")
    
