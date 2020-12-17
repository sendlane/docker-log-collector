from datetime import date, timedelta, datetime
import gzip
import json
import io
import traceback
import csv
import re
from collections import defaultdict
import argparse
import pprint
import math
import pytz
from io import StringIO
from hyperloglog.hll import HyperLogLog
from sendlane.google.sheets.api import GoogleSheet

gs = GoogleSheet()
sheet = gs.get_sheet()

parser = argparse.ArgumentParser(description='Run reporting for trend')
parser.add_argument('--date', type=str, dest="date", default=None, help='Supply the date you want to report on')
parser.add_argument('--append', action='store_true', dest="append", help='Append stats data to trend spreadsheet')
args = parser.parse_args()

print(args.date)
report_date = None
if args.date is not None:
    year,month,day = [ int(x) for x in args.date.split('-')]
    report_date = date(year,month,day)
else:
    report_date = date.today() - timedelta(days=1)

print(report_date)

print(f"{report_date.year} / {report_date.month} / {report_date.day}")

stats = defaultdict( lambda: defaultdict( lambda: defaultdict( lambda: dict( count=0, unique=HyperLogLog(0.1)))))

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

for topic in ['trend_email_sent_log','trend_email_delivered_log','trend_email_bounce_log','trend_email_feedback_log','trend_email_open_log','trend_email_click_log']:
    base_dir = f"/logs/archive/logs/daily/{topic}/{report_date.year:04}/{report_date.month:02}/{report_date.day:02}/"
    # /logs/archive/logs/daily/trend_email_sent_log/2020/10/12/trend_email_sent_log-20201012.log.gz
    filename = base_dir + topic + f"-{report_date.year:04}{report_date.month:02}{report_date.day:02}.log.gz"
    print(filename)

    try:
        with io.TextIOWrapper(io.BufferedReader(gzip.open(filename))) as file:
            for line in file:
                line = line.rstrip()
                if topic == 'trend_email_sent_log':
                    data = json.loads(line)
                    report_date = report_date
                    email = data['to_email']
                    if re.search(r"@mx-test.xyz",email):
                        continue
                    job_id = data['job_id']
                    send_timestamp = data['timestamp']
                    timezoneobject = pytz.timezone('America/New_York')
                    datetimeobject = datetime.fromtimestamp(send_timestamp)
                    send_datetime = timezoneobject.localize(datetimeobject)
                    send_date = send_datetime.strftime('%Y-%m-%d')
                    stats[job_id][send_date]['sent']['count'] += 1
                    stats[job_id][send_date]['sent']['unique'].add(email)
                elif topic == 'trend_email_delivered_log':
                    send_date = report_date.strftime('%Y-%m-%d')
                    hostname, data = line.split("\t")
                    f = StringIO(data)
                    reader = csv.reader(f, delimiter=',')
                    row = next(reader)
                    email = row[4]
                    job_id = row[18]
                    if re.search(r"@mx-test.xyz",email):
                        continue
                    if email == 'rcpt':
                        continue
                    stats[job_id][send_date]['delivered']['count'] += 1
                elif topic == 'trend_email_bounce_log':
                    send_date = report_date.strftime('%Y-%m-%d')
                    hostname, data = line.split("\t")
                    f = StringIO(data)
                    reader = csv.reader(f, delimiter=',')
                    row = next(reader)
                    email = row[4]
                    if re.search(r"@mx-test.xyz",email):
                        continue
                    status = row[7]
                    job_id = row[18]
                    if email == 'rcpt':
                        continue
                    bounce_type = bounce_rules.get(status,None)
                    print(f"{email} : {status} : {job_id} : {bounce_type}")
                    if bounce_type == 'hard':
                        stats[job_id][send_date]['bounced']['count'] += 1
                        stats[job_id][send_date]['bounced']['unique'].add(email)
                elif topic == 'trend_email_open_log':
                    data = json.loads(line)
                    report_date = report_date
                    email = data['to_email']
                    if re.search(r"@mx-test.xyz",email):
                        continue
                    job_id = data['job_id']
                    send_timestamp = data['send_timestamp']
                    timezoneobject = pytz.timezone('America/New_York')
                    datetimeobject = datetime.fromtimestamp(send_timestamp)
                    send_datetime = timezoneobject.localize(datetimeobject)
                    send_date = send_datetime.strftime('%Y-%m-%d')
                    stats[job_id][send_date]['opened']['count'] += 1
                    stats[job_id][send_date]['opened']['unique'].add(email)
                elif topic == 'trend_email_click_log':
                    data = json.loads(line)
                    report_date = report_date
                    email = data['to_email']
                    if re.search(r"@mx-test.xyz",email):
                        continue
                    job_id = data['job_id']
                    send_timestamp = data['send_timestamp']
                    timezoneobject = pytz.timezone('America/New_York')
                    datetimeobject = datetime.fromtimestamp(send_timestamp)
                    send_datetime = timezoneobject.localize(datetimeobject)
                    send_date = send_datetime.strftime('%Y-%m-%d')
                    stats[job_id][send_date]['clicked']['count'] += 1
                    stats[job_id][send_date]['clicked']['unique'].add(email)
                elif topic == 'trend_email_feedback_log':
                    pass
    except Exception as exc:
        traceback.print_exc()
        pass

stats_rows = []
print("\t".join(['report_date','job_id','send_date','sent','delivered','bounced','feedback','opened','clicked','unique_bounced','unique_feedback','unique_opened','unique_clicked']))
for job_id in stats.keys():
    for send_date in stats[job_id].keys():
        send_stats = [ str(stats[job_id][send_date][x]['count']) if x in stats[job_id][send_date] else str(0) for x in ['sent','delivered','bounced','feedback','opened','clicked']]
        unique_stats = [ str(math.floor(stats[job_id][send_date][x]['unique'].card())) if x in stats[job_id][send_date] else str(0) for x in ['bounced','feedback','opened','clicked']]
        print(f"{report_date}\t{job_id}\t{send_date}\t" + "\t".join(send_stats) + "\t" + "\t".join(unique_stats))
        stats_rows.append([str(report_date),job_id,str(send_date),*send_stats,*unique_stats])

if args.append:
    print(stats_rows)
    gs.append(stats_rows)
