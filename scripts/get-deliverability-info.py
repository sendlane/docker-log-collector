import sys
import pymysql
import json
import datetime

dbh = pymysql.connect(db="deliverability",host="198.154.86.66",port=3306,user="targetly",password="Cn933pT38",cursorclass=pymysql.cursors.DictCursor, autocommit=True)
dbh2 = pymysql.connect(db="argetly",host="198.154.86.66",port=3306,user="targetly",password="Cn933pT38",cursorclass=pymysql.cursors.DictCursor, autocommit=True)
cursor = dbh.cursor()
cursor2 = dbh2.cursor()

stats = dict(deliver=0,nodeliver=0,unknown=0)

count = 0
for line in sys.stdin:
    count += 1

    try:
        data = line.split("\t")
        email = data[4]
        #print(data[18])
        domain,type,newsletter_id,checksum = data[18].split('_')
        cursor.execute("select * from profile where email = %s",[email])
        row = cursor.fetchone()
        # {'email': 'rwood65@yahoo.com', 'last_sent': datetime.date(2020, 4, 18), 'last_open': datetime.date(2020, 3, 24), 'last_click': datetime.date(2020, 3, 24), 'last_bounce': datetime.date(2020, 2, 29), 'last_complaint': None, 'last_deliverable': None, 'is_deliverable': 1, 'is_quarantined': 0, 'soft_bounce_count': 0, 'hard_bounce_count': 0, 'complaint_count': None, 'created_at': datetime.datetime(2019, 8, 10, 10, 8, 47), 'updated_at': datetime.datetime(2020, 4, 18, 10, 18, 15), 'last_spam_bounce': None, 'spam_bounce_count': 8, 'profile_id': 9135200}
    
    
        cursor2.execute(f"use argetly_{domain}")
        cursor2.execute("select * from newsletter_subscribe where email = %s",[email])
        subscriber = cursor2.fetchone()
    
        data = dict(email=email,domain=domain,days_since_created=None,days_since_opened=None,is_deliverable=None)
    
        if subscriber is not None and 'created_at' in subscriber and subscriber['created_at'] is not None:
            days_since_created = (datetime.datetime.now() - subscriber['created_at']).days
            data['days_since_created'] = days_since_created
        
        if row and 'last_open' in row and row['last_open'] is not None:
            days_since_opened = (datetime.date.today() - row['last_open']).days
            data['days_since_opened'] = days_since_opened
    
        if row and 'is_deliverable' in row:
            data['is_deliverable'] = row['is_deliverable']
    
        if 'days_since_opened' in data and data['days_since_opened'] is not None and data['days_since_opened'] < 90:
            stats["deliver"] += 1
        elif 'days_since_created' in data and data['days_since_created'] is not None and data['days_since_created'] < 30:
            stats["deliver"] += 1
        else:
            stats["nodeliver"] += 1
    except Exception as exc:
        stats['unknown'] += 1
    
    if count % 1000 == 0:
        print(stats)
