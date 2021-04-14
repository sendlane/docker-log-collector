
import os
import sys
import gzip
import math
from datetime import datetime, date, timedelta
import os.path
import traceback
import logging
import json

class InvalidStartEndDates(Exception):
    pass

class TopicRequired(Exception):
    pass

class DateTimeRequired(Exception):
    pass

class DateRequired(Exception):
    pass

def converter(o):
    if isinstance(o, datetime):
        return o.__str__()

class LogFinder:

    def __init__(self, topic=None, start=None, end=None, interval="daily", inclusive=False ):
        if start is None and end is None:
            raise InvalidStartEndDates("LogFinder::find_logs must have a valid start and end date")

        if topic is None:
            raise TopicRequired("topic is a required parameter")

        if interval not in ['hourly','daily']:
            raise InvalidInterval("interval must be in ['daily','hourly']")

        self.log = logging.getLogger(self.__class__.__name__)

        self.archive_base_dir = os.environ.get("LOG_ARCHIVE_BASE_DIR","/logs/archive/logs")
        self.log_base_dir = os.environ.get("LOG_BASE_DIR","/logs")
        self.interval = interval
        self.topic = topic
        self.start = start
        self.end = end
        self.inclusive = inclusive

    def find_logs(self):
        topic = self.topic
        start = self.start
        end = self.end
        interval = self.interval
        inclusive = self.inclusive
        files = []
        if interval == 'daily':
            days = self.get_days_between(start,end, inclusive=inclusive)
            for day in days:
                log_file = os.path.join(self.log_base_dir,interval,topic + "-" + day.strftime("%Y%m%d") + ".log")
                archive_log_file = os.path.join(self.archive_base_dir,interval,topic,str(day.year).zfill(4),str(day.month).zfill(2),str(day.day).zfill(2), topic + "-" + day.strftime("%Y%m%d") + ".log.gz")
                files.append( dict(locator=day, log=log_file, archive=archive_log_file) )
        elif interval == 'hourly':
            hours = self.get_hours_between(start,end, inclusive=inclusive)
            for hour in hours:
                log_file = os.path.join(self.log_base_dir,interval,topic + "-" + hour.strftime("%Y%m%d-%H") + ".log")
                archive_log_file = os.path.join(self.archive_base_dir,interval,topic,str(hour.year).zfill(4),str(hour.month).zfill(2),str(hour.day).zfill(2), topic + "-" + hour.strftime("%Y%m%d-%H") + ".log.gz")
                files.append( dict(locator=hour, log=log_file, archive=archive_log_file) )

        return files

    def get_file_handles(self, files=[]):
        found = []
        missing = []
        errors = []
        file_handles = []
        for file in files:
            log_file = file['log']
            archive_log_file = file['archive']
            print(log_file)
            print(archive_log_file)
            if os.path.isfile(log_file) is True:
                self.log.debug(f"Sending uncompressed file: {log_file}")
                try:
                    fh = open(log_file,'rb')
                    file_handles.append(fh)
                    found.append(file)
                    have_valid_handle = True
                except Exception as exc:
                    self.log.error(traceback.format_exc())
                    errors.append( dict(info=file, exception=traceback.format_exc()) )
            elif os.path.isfile(archive_log_file):
                self.log.debug(f"Sending compressed file: {archive_log_file}")
                try:
                    fh = gzip.open(archive_log_file,'rb')
                    file_handles.append(fh)
                    found.append(file)
                except Exception as exc:
                    self.log.error(traceback.format_exc())
                    errors.append( dict(info=file, exception=traceback.format_exc()) )
            else:
                self.log.error("Missing a file: " + json.dumps(file, default=converter))
                missing.append( dict( info=file ) )

        return [file_handles, missing, errors]


    def stream_all_logs(self):

        logs = self.find_logs()
        file_handles, missing, errors = self.get_file_handles(files=logs)
        
        self.log.debug("######### Errors #########")
        self.log.debug(errors)
        self.log.debug("######### Missing #########")
        self.log.debug(missing)
        self.log.debug("######### Filehandles ##########")
        self.log.debug(file_handles)
        self.log.debug("######### DATA ##########")

        for file_handle in file_handles:
            try:
                for line in file_handle:
                    yield line.decode('utf8')
            except Exception as exc:
                traceback.print_exc()
                pass
        return

    def get_days_between(self,start,end, inclusive=False):

            if type(start) is not date or type(end) is not date:
                raise DateRequired("start and end must be datetime objects")

            delta_days = ( date(end.year,end.month,end.day) - date(start.year,start.month,start.day) ).days

            if inclusive is True:
                delta_days += 1

            days = []
            for x in range(delta_days):
                days.append(start + timedelta(days=x))
            return days

    def get_hours_between(self,start,end, inclusive=False):

            if type(start) is not datetime or type(end) is not datetime:
                raise DateTimeRequired("start and end must be datetime objects")

            delta = end - start
            delta_hours = math.floor(delta.days * 24 + delta.seconds / 60.0 / 60.0)

            if inclusive is True:
                delta_hours += 1

            hours = []
            for x in range(delta_hours):
                hours.append(start + timedelta(hours=x) )

            return hours


    

if __name__ == '__main__':
    
    #print(LogFinder(topic="email_delivered_log", start=date(2020,4,1), end=date(2020,4,1), interval="daily", inclusive=False).find_logs())
    #print(LogFinder(topic="email_delivered_log", start=date(2020,4,1), end=date(2020,4,1), interval="daily", inclusive=True).find_logs())
    #print(LogFinder(topic="email_delivered_log", start=date.today(), end=date.today(), interval="daily", inclusive=True).find_logs())

    #print(LogFinder(topic="email_open_log", start=datetime(2020,4,1,0), end=datetime(2020,4,1,0), interval="hourly", inclusive=False).find_logs())
    #print(LogFinder(topic="email_open_log", start=datetime(2020,4,1,0), end=datetime(2020,4,2,8), interval="hourly", inclusive=False).find_logs())
    #print(LogFinder(topic="email_open_log", start=datetime(2020,4,1,0), end=datetime(2020,4,2,0), interval="hourly", inclusive=False).find_logs())
    #print(LogFinder(topic="email_open_log", start=datetime(2020,4,1,0), end=datetime(2020,4,1,0), interval="hourly", inclusive=True).find_logs())
    #print(LogFinder(topic="email_open_log", start=datetime(2020,4,1,0), end=datetime(2020,4,2,8), interval="hourly", inclusive=True).find_logs())
    #print(LogFinder(topic="email_open_log", start=datetime(2020,4,1,0), end=datetime(2020,4,2,0), interval="hourly", inclusive=True).find_logs())
    #print(LogFinder(topic="email_open_log", start=datetime.now(), end=datetime.now(), interval="hourly", inclusive=True).find_logs())

    for line in LogFinder(topic="email_click_log", start=datetime.now() - timedelta(hours=1), end=datetime.now() - timedelta(hours=1), interval="hourly", inclusive=True).stream_all_logs():
        print(line)
