from datetime import date, timedelta, datetime
import gzip
import logging
import sys
import json
import time
import io
import traceback
import csv
import re
import os
import os.path
from collections import defaultdict
import argparse
import math
import pytz
from io import StringIO
from hyperloglog.hll import HyperLogLog
import psycopg2
from psycopg2.extras import DictCursor
from dateutil import parser as date_parser

parser = argparse.ArgumentParser(description='Run reporting for trend')
parser.add_argument('--date', type=str, dest="date", default=None, help='Supply the date you want to report on')
parser.add_argument('--append', action='store_true', dest="append", help='Append stats data to trend spreadsheet')
args = parser.parse_args()

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

root.info(args.date)
report_date = None
if args.date is not None:
    year,month,day = [ int(x) for x in args.date.split('-')]
    report_date = date(year,month,day)
else:
    report_date = date.today() - timedelta(days=1)

root.info(report_date)

root.info(f"{report_date.year} / {report_date.month} / {report_date.day}")

total_records = 0
stats = defaultdict( lambda: defaultdict( lambda: defaultdict( lambda: defaultdict( lambda: dict( count=0, unique=HyperLogLog(0.1))))))

class KillFileRun(Exception):
    pass

bounce_rules = {
    'bad-mailbox': 'hard',
    'invalid-mailbox': 'hard',
    'bad-domain': 'hard',
    'quota-issues': 'hard',
    'inactive-mailbox': 'hard',
    'bad-connection': 'hard',
    'spam-related': 'spam',
    'policy-related': 'spam',
    'message-expired': 'spam',
    'routing-errors': 'soft',
    'no-answer-from-host': 'soft',
    'content-related': 'spam',
    'protocol-errors': 'soft',
    'relaying-issues': 'soft'
}

filetypes = [
    dict(prefix='email_sent_log', interval="hourly"),
    dict(prefix='email_delivered_log', interval="daily"),
    dict(prefix='email_bounce_log', interval="daily"),
    dict(prefix='email_feedback_log', interval="daily"),
    dict(prefix='email_open_log', interval="hourly"),
    dict(prefix='email_click_log', interval="hourly"),
]

def get_send_date(date):
    timezoneobject = pytz.timezone('America/New_York')
    try:
        send_datetime = date_parser.isoparse(date).astimezone(timezoneobject)
        send_date = send_datetime.strftime('%Y-%m-%d')
    except ValueError as exc:
        send_datetime = timezoneobject.localize(datetime.fromisoformat(date['date']))
        send_date = send_datetime.strftime('%Y-%m-%d')
        pass

    return send_date

def process_line(line,topic):
    try:
        line = line.rstrip()
        if topic == 'email_sent_log':
            data = json.loads(line)
            domain = data['domain']
            email = data['subscriber']['email']
            newsletter_id = data['newsletter']['id']
            send_timestamp = data['timestamp']
            send_datetime = datetime.fromisoformat(send_timestamp)
            send_date = send_datetime.strftime('%Y-%m-%d')
            stats[domain][int(newsletter_id)][send_date]['sent']['count'] += 1
            stats[domain][int(newsletter_id)][send_date]['sent']['unique'].add(email)
        elif topic == 'email_delivered_log':
            send_date = report_date.strftime('%Y-%m-%d')
            hostname, data = line.split("\t")
            f = StringIO(data)
            reader = csv.reader(f, delimiter=',')
            row = next(reader)
            root.debug(row)
            email = row[4]
            job_id = row[18]
            vmta = row[17]
            domain, type, newsletter_id = job_id.split("_")
            if email == 'rcpt':
                return
            root.debug(f"{domain}\t{newsletter_id}\t{send_date}")

            if vmta in ['Shredder','Klean13','Inactive','ZakDestroy']:
                stats[domain][int(newsletter_id)][send_date]['block']['count'] += 1
            else:
                stats[domain][int(newsletter_id)][send_date]['delivered']['count'] += 1
        elif topic == 'email_bounce_log':
            send_date = report_date.strftime('%Y-%m-%d')
            hostname, data = line.split("\t")
            f = StringIO(data)
            reader = csv.reader(f, delimiter=',')
            row = next(reader)
            root.debug(row)
            email = row[4]
            status = row[7]
            job_id = row[18]
            root.debug(job_id)
            domain, type, newsletter_id = job_id.split("_")
            if email == 'rcpt':
                return
            bounce_type = bounce_rules.get(status,None)
            root.debug(f"{domain}: {newsletter_id} : {email} : {status} : {bounce_type}")
            if bounce_type == 'hard':
                stats[domain][int(newsletter_id)][send_date]['hard_bounce']['count'] += 1
                stats[domain][int(newsletter_id)][send_date]['hard_bounce']['unique'].add(email)
            elif bounce_type == 'soft':
                stats[domain][int(newsletter_id)][send_date]['soft_bounce']['count'] += 1
                stats[domain][int(newsletter_id)][send_date]['soft_bounce']['unique'].add(email)
            elif bounce_type == 'spam':
                stats[domain][int(newsletter_id)][send_date]['spam_bounce']['count'] += 1
                stats[domain][int(newsletter_id)][send_date]['spam_bounce']['unique'].add(email)
        elif topic == 'email_open_log':
            data = json.loads(line)
            email = data['email']
            domain = data['domain']
            newsletter_id = data['newsletter_id']
            send_date = get_send_date(data['sent_time'])
            stats[domain][int(newsletter_id)][send_date]['open']['count'] += 1
            stats[domain][int(newsletter_id)][send_date]['open']['unique'].add(email)
        elif topic == 'email_click_log':
            data = json.loads(line)
            email = data['email']
            domain = data['domain']
            newsletter_id = data['newsletter_id']
            send_timestamp = data['timestamp']
            datetimeobject = datetime.fromisoformat(send_timestamp)
            send_date = datetimeobject.strftime('%Y-%m-%d')

            unsubscribe = data['unsubscribe']

            if unsubscribe == 1:
                stats[domain][int(newsletter_id)][send_date]['unsubscribe']['count'] += 1
                stats[domain][int(newsletter_id)][send_date]['unsubscribe']['unique'].add(email)
            else:
                stats[domain][int(newsletter_id)][send_date]['click']['count'] += 1
                stats[domain][int(newsletter_id)][send_date]['click']['unique'].add(email)
        elif topic == 'email_feedback_log':
            # {"link":"unsubscribe","newsletterId":25,"subscriberId":98344,"domain":"knifevoyager","time":1618272328,"del":6,"email":"snowfootin@yahoo.com","one_click":1,"Received":"from 10.253.31.92\r\n by atlas320.free.mail.gq1.yahoo.com with HTTPS; Tue, 13 Apr 2021 00:05:33 +0000","Sender":"knifevoyager@sldelsrv.com","Message-ID":"117-knifevoyager-autoseries-25-98344-ff@knifevoyager.sldelsrv.com","Date":"Mon, 12 Apr 2021 20:05:28 -0400","Subject":"[Gun Contest Co Sponsor-2] Hello From The Tunnel Rat...","From":"support@tunnelratsurvival.com","List-Unsubscribe":"<https:\/\/knifevoyager.slrcdn.com\/track\/click?linkData=1-8105eyJsaW5rIjoidW5zdWJzY3JpYmUiLCJuZXdzbGV0dGVySWQiOjI1LCJzdWJzY3JpYmVySWQiOjk4MzQ0LCJkb21haW4iOiJrbmlmZXZveWFnZXIiLCJ0aW1lIjoxNjE4MjcyMzI4LCJkZWwiOjYsImVtYWlsIjoic25vd2Zvb3RpbkB5YWhvby5jb20iLCJvbmVfY2xpY2siOjF9>","newsletter_id":25,"autoseries_id":null,"subscribe_id":98344,"mailerKey":"SPAM_REPORTS_206_2021-04-12","timestamp":"2021-04-12T23:51:48-04:00"}
            data = json.loads(line)
            email = data['email']
            domain = data['domain']
            newsletter_id = data['newsletterId']
            send_timestamp = data['timestamp']
            datetimeobject = datetime.fromisoformat(send_timestamp)
            send_date = datetimeobject.strftime('%Y-%m-%d')
            stats[domain][int(newsletter_id)][send_date]['feedback']['count'] += 1
            stats[domain][int(newsletter_id)][send_date]['feedback']['unique'].add(email)
            pass
    
        return

    except Exception as exc:
        root.info(traceback.format_exc())
        return

def write_stats():
    global total_records
    global stats
    with open("/usr1/volumes/code/sql/stats/output-" + report_date.strftime('%Y-%m-%d') + ".tsv","w") as fh:
        fh.write("\t".join(['report_date','domain','job_id','send_date','sent','delivered','block','hard_bounce','soft_bounce','spam_bounce','feedback','open','click','unsubscribe','unique_hard_bounce','unique_soft_bounce','unique_spam_bounce','unique_feedback','unique_open','unique_click','unique_unsubscribe']) + "\n")
        for domain in stats.keys():
            for job_id in stats[domain].keys():
                for send_date in stats[domain][job_id].keys():
                    send_stats = [ str(stats[domain][job_id][send_date][x]['count']) if x in stats[domain][job_id][send_date] else str(0) for x in ['sent','delivered','block','hard_bounce','soft_bounce','spam_bounce','feedback','open','click','unsubscribe']]
                    unique_stats = [ str(math.floor(stats[domain][job_id][send_date][x]['unique'].card())) if x in stats[domain][job_id][send_date] else str(0) for x in ['hard_bounce','soft_bounce','spam_bounce','feedback','open','click','unsubscribe']]
                    fh.write(f"{report_date}\t{domain}\t{job_id}\t{send_date}\t" + "\t".join(send_stats) + "\t" + "\t".join(unique_stats) + "\n")
                    total_records += 1
    return

def load_stats():
    global total_records
    table_name = "daily_send_report"
    params = dict(database="stats",
              host="docker02.sendlane.int",
              port=5432,
              user="postgres",
              password="dbadmin",
              cursor_factory=DictCursor)
    dbh = psycopg2.connect(**params)
    cursor = dbh.cursor()

    cursor.execute(f"delete from {table_name} where report_date = %s", [report_date.strftime('%Y-%m-%d')])
    dbh.commit()

    with open("/usr1/volumes/code/sql/stats/output-" + report_date.strftime('%Y-%m-%d') + ".tsv","r") as fh:
        sql = f"COPY {table_name}(report_date,domain,newsletter_id,send_date,sent,delivered,block,hard_bounce,soft_bounce,spam_bounce, feedback,open,click,unsubscribe, unique_hard_bounce, unique_soft_bounce, unique_spam_bounce, unique_feedback, unique_open, unique_click, unique_unsubscribe) from STDIN WITH DELIMITER E'\t' CSV HEADER"
        cursor.copy_expert(sql, fh)

    cursor.execute(f"select count(*) from {table_name} where report_date = %s", [report_date.strftime('%Y-%m-%d')])
    row = cursor.fetchone()
    if row['count'] == total_records:
        print(f"Successfully bulk inserted {total_records} into the stats db")
        dbh.commit()
    else:
        raise Exception(f"Failed to load all the records from the stats run for {report_date} ( {row['count']} / {total_records} )")
        dbh.rollback()


for type in filetypes:
    topic = type['prefix']
    interval = type['interval']
    base_dir = f"/usr1/volumes/log-collector/logs/archive/logs/{interval}/{topic}/{report_date.year:04}/{report_date.month:02}/{report_date.day:02}/"
    #base_dir = f"/logs/{interval}/{topic}/{report_date.year:04}/{report_date.month:02}/{report_date.day:02}/"
    # /logs/archive/logs/daily/email_sent_log/2020/10/12/email_sent_log-20201012.log.gz
    files = []
    if interval == 'daily':
        files.append(base_dir + topic + f"-{report_date.year:04}{report_date.month:02}{report_date.day:02}.log.gz")
    elif interval == 'hourly':
        allfiles = [ base_dir + '/' + f for f in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, f)) and re.search(f"{topic}-(.*?).log.gz", os.path.join(base_dir, f)) is not None]
        files.extend(allfiles)
    else:
        raise(Exception("Invalid type so not able to find the files"))

    try:
        for file in files:
            counter = 0
            root.info(f"######## Processing {file}")
            try:
                with io.TextIOWrapper(io.BufferedReader(gzip.open(file))) as fh:
                    root.info(f"Reading {file}")
                    while(True):
                        try:
                            line = fh.readline()
                            if line is None or line == '':
                                break
                        except Exception as exc:
                            root.info(traceback.format_exc())
                            continue
                        counter += 1
                        #if counter == 250:
                        #    raise KillFileRun()
                        process_line(line,topic)
            except KillFileRun as exc:
                pass
            root.info(f"Done with {counter} lines in {file}")
    except Exception as exc:
        root.info(traceback.format_exc())
        pass
    
write_stats()
load_stats()
