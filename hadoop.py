"""
Gather job histories from hadoop log into table for R
"""

import os
import gzip
import csv
import datetime
import urlparse

import boto

import settings

HADOOP_LOG_DIR = '/var/log/hadoop/'
JOB_INFO_FIELDS = ['dt', 'status', 'mapSlotSeconds', 'reduceSlotSeconds',
                   'numMaps', 'numReduces',
                   'submitTime', 'launchTime', 'finishTime',
                   'jobName', 'jobType']


def history_files():
    files = os.listdir(HADOOP_LOG_DIR)
    for f in files:
        if f.startswith('mapred-hadoop-historyserver') and f.endswith('.gz'):
            yield os.path.join(HADOOP_LOG_DIR, f)


def read_job_summary_lines(f):
    job_summary_pattern = 'org.apache.hadoop.mapreduce.jobhistory.JobSummary'
    with gzip.open(f,'r') as fin:
        for line in fin:
            if line.find(job_summary_pattern) >= 0:
                yield line


def parse_job_summary(line):
    job_info_dict = {}

    job_info_dict['dt'] = line[:24]

    job_info = line[115:]
    for pair in job_info.split(','):
        try:
            (k,v) = pair.split('=')
            job_info_dict[k] = v
        except ValueError:
            # jobName can contain '=' in value part
            pass

    # jobName might contain commas and =
    jobname_key = 'jobName='
    jobname_index = job_info.find(jobname_key)
    jobname = job_info[jobname_index+len(jobname_key):].strip()
    job_info_dict['jobName'] = jobname
    job_info_dict['jobType'] = jobname.split('(')[0]

    return job_info_dict


def filter_job_info(job_info):
    # remove unwanted keys before passing to csv.DictWriter
    filtered_info = { k: job_info[k] for k in JOB_INFO_FIELDS }
    return filtered_info


def upload_to_s3(filename):
    S3_KEY_PREFIX = 'metrics/hadoop-jobs/'
    s3_connection = boto.connect_s3()
    bucketname = urlparse.urlsplit(settings.S3_URL).netloc
    basename = os.path.basename(filename)
    keyname = os.path.join(S3_KEY_PREFIX, basename)
    bucket = s3_connection.get_bucket(bucketname)
    key = boto.s3.key.Key(bucket, keyname)
    key.set_contents_from_filename(filename)


def main():

    HISTORY_FILE = os.path.join(settings.WORKDIR,
        'history-{timestamp}.csv.gz'.format(
            timestamp=datetime.datetime.now().strftime('%Y-%m-%d')
        ))

    with gzip.open(HISTORY_FILE, 'wb') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=JOB_INFO_FIELDS, dialect='excel')

        writer.writeheader()

        for f in history_files():
            for line in read_job_summary_lines(f):
                job_info = filter_job_info(parse_job_summary(line))
                writer.writerow(job_info)

    upload_to_s3(HISTORY_FILE)







if __name__ == "__main__":
    main()

