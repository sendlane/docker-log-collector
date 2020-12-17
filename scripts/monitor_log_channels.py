import requests
import traceback
import json
import urllib
import multiprocessing, time, signal
import subprocess
import logging
import sys
import os

from multiprocessing import Pool, Process

def get_topics():
  target_topics = {}
  lookupd_hosts = os.environ.get("NSQLOOKUPD_HOSTS").split(',')
  for lookupd_host in lookupd_hosts:
    topic_data = requests.get("http://" + lookupd_host + "/topics").json()
    topics = topic_data['topics']
    for topic in topics:
      channel_data = requests.get('http://' + lookupd_host + '/channels?topic=' + urllib.parse.quote(topic)).json()
      channels = channel_data['channels']
      for channel in channels:
        if channel == 'nsq_to_file':
          target_topics[topic] = dict(type="daily")
        if channel == 'nsq_to_file_hourly':
          target_topics[topic] = dict(type="hourly")
  return target_topics

def run_logger(topic=None,type="daily"):
  permanent_log_dir = os.environ.get("LOG_COLLECTION_BASE_DIRECTORY")
  args = [
    "nsq_to_file",
    "--max-in-flight=2000",
    "-output-dir=" + permanent_log_dir + '/daily',
    "--channel=nsq_to_file",
    "--topic",
    topic,
    "-datetime-format=%Y%m%d",
    "-filename-format=<TOPIC><REV>-<DATETIME>.log"
  ]

  if type == 'hourly':
    args[2] = "-output-dir=" + permanent_log_dir + '/hourly'
    args[3] = "--channel=nsq_to_file_hourly"
    args[6] = "-datetime-format=%Y%m%d-%H"

  print("----- starting new logger for %s -----" % (topic,))
  print("Type is : %s" % (type,))
  print(args)
  lookupd_hosts = os.environ.get("NSQLOOKUPD_HOSTS").split(',')
  for lookupd_host in lookupd_hosts:
    args.append("-lookupd-http-address=" + lookupd_host)
  #devnull = open('/dev/null','w')
  devnull = sys.stdout
  subprocess.call(args, stdout=devnull, stderr=devnull)

if __name__ == "__main__":
  running = {}
  while True:
    try:
      target_topics = get_topics()
      total_targets = len(target_topics)
      for topic in target_topics:
        if topic in running:
          if running[topic].is_alive():
            continue
          else:
            del running[topic]
        p = Process(target=run_logger,args=(topic,target_topics[topic].get("type","daily")))
        p.start()
        running[topic] = p
    except Exception as exc:
      print(traceback.print_exc())
      print("Never die on an unexpected exception")
    time.sleep(5)
