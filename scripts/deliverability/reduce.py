import json
import gzip
from collections import defaultdict
from io import StringIO
import csv
import time
import hyperloglog
import sys
import pprint
import argparse
from os import listdir
from os.path import isfile, join
import datetime
import re

parser = argparse.ArgumentParser(description='Deliverability stats run')
parser.add_argument('-d','--date', type=str, help='date to process', dest='date')
args = parser.parse_args()

if args.date is None:
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    args.date = yesterday.strftime('%Y-%m-%d')

outfh = open(f"/logs/scripts/deliverability/output/{args.date}-domain.log","w")
topoutfh = open(f"/logs/scripts/deliverability/output/{args.date}-toplevel.log","w")

year,month,day = args.date.split('-')

delivered_log = f"/logs/archive/logs/daily/email_delivered_log/{year}/{month}/{day}/email_delivered_log-{year}{month}{day}.log.gz"

#stats = dict(
#    domain = dict(
#        newsletter_id = dict(
#            delivered = 0,
#            total_opens = 0,
#            unique_opens = hyperloglog.HyperLogLog(0.01)
#        )
#    )
#)

topstats = defaultdict( lambda: defaultdict( lambda: dict(delivered=0, total_opens=0, unique_opens=hyperloglog.HyperLogLog(0.1) ) ))
stats = defaultdict( lambda: defaultdict( lambda: defaultdict( lambda: dict(delivered=0, total_opens=0, unique_opens=hyperloglog.HyperLogLog(0.1) ) )))

delivered = gzip.open(delivered_log)

hourly_file_path = f"/logs/archive/logs/hourly/email_open_log_extended/{year}/{month}/{day}"

print(listdir(hourly_file_path))

hourly_opens = [ hourly_file_path + '/' + f for f in listdir(hourly_file_path) if isfile(join(hourly_file_path, f))]

print(hourly_opens)

count = 0
for file in hourly_opens:
    fh = gzip.open(file, mode='r')
    for line in fh:
        count += 1
        data = json.loads(line)
        email = data['email']
        newsletter_id = data['newsletter_id']
        domain = data['domain']
        
        queue = data.get("queue",None)
        vmta = data.get("vmta",None)
        vmta_pool = data.get("vmta_pool",None)
        ip = data.get("ip",None)

        if queue is not None:
            domain_group,_ = queue.split('/')
            if domain_group in ['hotmail.com','yahoo.com','gmail.com','aol.com']:
                topstats["domain"][domain_group]['total_opens'] += 1
                topstats["domain"][domain_group]['unique_opens'].add(email)

                if vmta is not None:
                    stats["vmta"][vmta][domain_group]['total_opens'] += 1
                    stats["vmta"][vmta][domain_group]['unique_opens'].add(email)
        
                if vmta_pool is not None:
                    stats["vmta_pool"][vmta_pool][domain_group]['total_opens'] += 1
                    stats["vmta_pool"][vmta_pool][domain_group]['unique_opens'].add(email)
        
                if ip is not None:
                    stats["ip"][ip][domain_group]['total_opens'] += 1
                    stats["ip"][ip][domain_group]['unique_opens'].add(email)

        if vmta is not None:
            topstats["vmta"][vmta]['total_opens'] += 1
            topstats["vmta"][vmta]['unique_opens'].add(email)

        if vmta_pool is not None:
            topstats["vmta_pool"][vmta_pool]['total_opens'] += 1
            topstats["vmta_pool"][vmta_pool]['unique_opens'].add(email)

        if ip is not None:
            topstats["ip"][ip]['total_opens'] += 1
            topstats["ip"][ip]['unique_opens'].add(email)

        if count % 25000 == 0:
            print(f"Done with {count} open records in {file}")

print(f"Done with {count} open records")

delivered_count = 0
for line in delivered:
    hostname, data = line.strip().decode('utf8').split("\t")
    f = StringIO(data[2:-1])
    reader = csv.reader(f)
    for record in reader:
        delivered_count += 1
        ["b'd", '2020-04-03 03:02:19-0400', '2020-04-03 03:02:20-0400', 'bounce2@creatensend.com', 'jacbest@gmail.com', '2.0.0 (success)', 'smtp;250 2.0.0 OK 1585897340 i203si6503547ybg.272 - gsmtp', 'success', '', 'relayed', 'gmail-smtp-in.l.google.com (64.233.177.26)', 'smtp', '[127.0.0.1] (198.154.86.75)', 'smtp', '64.233.177.26', 'ENHANCEDSTATUSCODES,PIPELINING,CHUNKING,8BITMIME,SIZE,STARTTLS,SMTPUTF8', '15457', '192.186.128.4', 'personaldevelopment101_campaign_2658_49ad23d1ec9fa4bd8d77d02681df5cfa', '', 'gmail.com/192.186.128.4', 'creatensend', "Hurry, Grab It While It\\'s Free", '', '<00000143hldcj5gr-trx7naov-68c2-a635-f95c-6pxr78qzeyg4-000000@personaldevelopment101.creatensend.com>', "Precious Treasure <treasure@healthpedia.pro>'"]
        email = record[4]
        campaign = record[18]
        queue = record[20]
        vmta_pool = record[21] or 'Default'
        ip = record[26] if len(record) >= 27 else ''

        if queue is not None and re.search(r"\/",queue):
            domain_group, vmta = queue.split('/')
            if domain_group in ['hotmail.com','yahoo.com','gmail.com','aol.com']:
                topstats["domain"][domain_group]['delivered'] += 1
                stats["vmta"][vmta][domain_group]['delivered'] += 1
                stats["vmta_pool"][vmta_pool][domain_group]['delivered'] += 1
                stats["ip"][ip][domain_group]['delivered'] += 1

        topstats["vmta"][vmta]['delivered'] += 1
        topstats["vmta_pool"][vmta_pool]['delivered'] += 1
        topstats["ip"][ip]['delivered'] += 1

        if delivered_count % 25000 == 0:
            print(f"Done with {delivered_count} delivery records")

print(f"Done with {delivered_count} delivery records")

for level1 in stats:
    for level2 in stats[level1]:
        for level3 in stats[level1][level2]:
            delivered = stats[level1][level2][level3]['delivered']
            total_opens = stats[level1][level2][level3]['total_opens']
            unique_opens = len(stats[level1][level2][level3]['unique_opens'])
            outfh.write(f"{args.date}\t{level1}\t{level2}\t{level3}\t{delivered}\t{total_opens}\t{unique_opens}\n")

outfh.close()

for level1 in topstats:
    for level2 in topstats[level1]:
        delivered = topstats[level1][level2]['delivered']
        total_opens = topstats[level1][level2]['total_opens']
        unique_opens = len(topstats[level1][level2]['unique_opens'])

        topoutfh.write(f"{args.date}\t{level1}\t{level2}\t{delivered}\t{total_opens}\t{unique_opens}\n")

topoutfh.close()
