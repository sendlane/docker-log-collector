import os.path
import os
import pysftp
from datetime import datetime, timedelta

base_path = "/logs/archive/logs/hourly/klean13_feed"

with pysftp.Connection('64.227.108.28', username='alice', password='3dKPbCJJPsJVQ4sn') as sftp:

    last_hour = datetime.now() - timedelta(hours=2)
    year, month, day, hour = [ str(x) for x in [last_hour.year, last_hour.month, last_hour.day, last_hour.hour]]
    # klean13_feed-20200813-10.log.gz
    file_path = base_path + "/" + year.zfill(4) + "/" + month.zfill(2) + "/" + day.zfill(2)
    file_name = f"klean13_feed-{year.zfill(4)}{month.zfill(2)}{day.zfill(2)}-{hour.zfill(2)}.log.gz"
    file_location = file_path + "/" + file_name
    tmp_file_location = file_name + ".transferring"
    if os.path.isfile(file_location):
        print(f"Found file at {file_location}")
        sftp.put(file_location, tmp_file_location)
        if sftp.isfile(file_name):
            sftp.remove(file_name)
        sftp.rename(tmp_file_location, file_name)
