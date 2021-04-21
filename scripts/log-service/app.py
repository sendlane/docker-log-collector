import sys

import json
import traceback
import inspect
import os
import base64
from datetime import datetime,timedelta, date

from lib.log_finder import LogFinder, converter

from flask import Flask, Blueprint, flash, g, session, render_template, send_from_directory, redirect, url_for, request, make_response, Response, send_file

app = Flask(__name__)

@app.errorhandler(404)
def not_found(error):
    return ('', 404)

@app.route('/stream/logs/<path:path>', methods=['POST'])
def stream_logs(path):

    form = request.get_json()
    print(path)
    print(form)
    interval = path.split('/')[0]
    topic = form.get("topic")

    start_year, start_month, start_day, start_hour = [
        int(form.get("start_year")) if form.get("start_year",None) is not None else None,
        int(form.get("start_month")) if form.get("start_month",None) is not None else None,
        int(form.get("start_day")) if form.get("start_day",None) is not None else None,
        int(form.get("start_hour")) if form.get("start_hour",None) is not None else None,
    ]
    end_year, end_month, end_day, end_hour = [
        int(form.get("end_year")) if form.get("end_year",None) is not None else None,
        int(form.get("end_month")) if form.get("end_month",None) is not None else None,
        int(form.get("end_day")) if form.get("end_day",None) is not None else None,
        int(form.get("end_hour")) if form.get("end_hour",None) is not None else None,
    ]

    print(start_year, start_month, start_day, start_hour)
    print(end_year, end_month, end_day, end_hour)

    inclusive = form.get("inclusive",False)

    start, end = [None,None]
    if interval == 'daily':
        start = date(start_year,start_month,start_day)
        end = date(end_year,end_month,end_day)
    elif interval == 'hourly':
        start = datetime(start_year,start_month,start_day,start_hour)
        end = datetime(end_year,end_month,end_day,start_hour)
    else:
        print("BROKEN NO INTERVAL DETERMINED")

    print(start)
    print(end)
        
    finder = LogFinder(
        topic=topic,
        start=start,
        end=end,
        inclusive=inclusive,
        interval=interval
    )

    return Response(finder.stream_all_logs(), mimetype='text/plain', headers={ "X-Files": json.dumps(finder.find_logs(), default=converter), "Content-type": "text/plain; charset=utf-8" } )

@app.route('/download/logs/<path:path>', methods=['POST'])
def download_log(path):

    form = request.get_json()
    print(path)
    interval = path.split('/')[0]
    topic = form.get("topic")

    start_year, start_month, start_day, start_hour = [
        int(form.get("year")) if form.get("year",None) is not None else None,
        int(form.get("month")) if form.get("month",None) is not None else None,
        int(form.get("day")) if form.get("day",None) is not None else None,
        int(form.get("hour")) if form.get("hour",None) is not None else None,
    ]

    start, end = [None,None]
    if interval == 'daily':
        start = date(start_year,start_month,start_day)
        end = date(start_year,start_month,start_day)
    elif interval == 'hourly':
        start = datetime(start_year,start_month,start_day,start_hour)
        end = datetime(start_year,start_month,start_day,start_hour)
    else:
        print("BROKEN NO INTERVAL DETERMINED")

    finder = LogFinder(
        topic=topic,
        start=start,
        end=end,
        inclusive=True,
        interval=interval
    )

    files = finder.find_logs()

    if len(files) == 1:
        json_string = json.dumps(files, default=converter)
        file = files[0]
        filepath = file['archive']
        filename = os.path.basename(file['archive'])
        try:
            # send_file(filename_or_fp, mimetype=None, as_attachment=False, attachment_filename=None, add_etags=True, cache_timeout=None, conditional=False, last_modified=None)
            return send_file(filepath, attachment_filename=filename, mimetype="application/gzip", as_attachment=True)
        except Exception as exc:
            return str(exc)
    else:
        return Response("No file found", 404)
