
import sys
import io
import json
import math
from datetime import datetime
from dateutil.tz import tzoffset
import pytz

for line in sys.stdin:
    data = json.loads(line.rstrip())
    if type(data) == list and len(data) > 0:
        data = data[0]
    else:
        continue

    # {'feedbacktype': '', 'addresser': 'updates@sendaze.com', 'diagnostictype': 'SMTP', 'timezoneoffset': '-0500', 'lhost': 'exim', 'destination': 'mbus.co.za', 'timestamp': 1607465584, 'senderdomain': 'sendaze.com', 'deliverystatus': '5.0.0', 'token': '551fec95a83db04eb900c64932896608d4e5c4c5', 'recipient': 'mjoseph@mbus.co.za', 'subject': 'Interview schedule: Your new 247 P/day gig.', 'origin': '<STDIN>', 'rhost': 'mx-02.uucpe.mweb.net', 'reason': 'onhold', 'diagnosticcode': "all hosts for 'mbus.co.za' have been failing for a long time (and retry time not reached)", 'messageid': '126-backend-autoseries-362-1418591-d4@backend.exact.sendaze.com', 'listid': '', 'action': 'failed', 'softbounce': 1, 'replycode': '', 'catch': None, 'alias': '', 'smtpagent': 'Exim', 'smtpcommand': ''}

    delivery_status = data.get("deliverystatus",None)
    diagnostict_code = data.get("diagnosticcode",None)
    reason = data.get("reason",None)
    message_id = data.get("messageid",None)
    subject = data.get("subject",None)
    from_address = data.get("addresser",None)
    senderdomain = data.get("senderdomain",None)
    timestamp = data.get("timestamp",None)
    # should always come in as America/New_York
    timezoneoffset = data.get("timezoneoffset",None)
    email = data.get('recipient',None)

    epoch_datetime = datetime.fromtimestamp(timestamp)
    epoch_datetime_tz = epoch_datetime.astimezone(pytz.timezone('America/New_York'))

    print(epoch_datetime)
    print(epoch_datetime_tz.isoformat())
    
    report_date = epoch_datetime_tz.strftime("%Y-%m-%d")

    print(report_date)

    

    print( "\t".join( [
        epoch_datetime_tz.isoformat(),
        email,
        delivery_status,
        diagnostict_code,
        reason,
        message_id,
        from_address,
        senderdomain,
        str(timestamp),
        timezoneoffset,
        subject,
    ]))
