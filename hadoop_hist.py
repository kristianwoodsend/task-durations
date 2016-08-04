"""
Gather job histories by walking hadoop log directory
and reading jhist files.
Store in csv file for R.
"""

import os
import sys
import gzip
import csv
import datetime
import urlparse
import json
import tarfile

import boto

import settings

HADOOP_LOG_DIR = '/var/log/hadoop/history/2016'
JOB_INFO_FIELDS = ['dt',
                   # 'status',
                   'mapSlotMs', 'reduceSlotMs', 'frameworkMs',
                   'numMaps', 'numReduces',
                   'submitTime', 'launchTime', 'finishTime',
                   'jobName',
                   # 'jobType'
                   ]


def history_files():
    for root, dirs, files in os.walk(HADOOP_LOG_DIR):
        for f in files:
            if f.endswith('.jhist'):
                hist_file = open(os.path.join(root, f), 'r')
                contents = hist_file.readlines()
                yield contents


def history_tar(tar_file):
    print tar_file
    tar = tarfile.open(tar_file)
    for member in tar.getmembers():
        if member.isfile() and member.name.endswith('.jhist'):
            f = tar.extractfile(member)
            contents = f.readlines()
            yield contents


def filter_job_info(job_info):
    # remove unwanted keys before passing to csv.DictWriter
    filtered_info = {k: job_info[k] for k in JOB_INFO_FIELDS}
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


def find(obj, key, value=None):
    # find the object that matches the key and value in obj
    if isinstance(obj, dict):
        try:
            if value is None:
                return obj[key]
            elif obj[key] == value:
                return obj

        except KeyError:
            # try values
            for child in obj.values():
                ret = find(child, key, value)
                if ret is not None:
                    return ret

    elif isinstance(obj, list):
        for child in obj:
            ret = find(child, key, value)
            if ret is not None:
                return ret

    return None


def to_json(l):
    try:
        return json.loads(l)
    except ValueError:
        return None


def read_jhist_file(contents):
    job_info = {}

    jlist = [to_json(l) for l in contents]
    jlist = [j for j in jlist if j is not None]

    job_info['jobName'] = find(find(jlist, 'type', 'JOB_SUBMITTED')['event'], 'jobName')

    job_info['submitTime'] = find(find(jlist, 'type', 'JOB_SUBMITTED')['event'], 'submitTime')
    job_info['launchTime'] = find(find(jlist, 'type', 'JOB_INITED')['event'], 'launchTime')

    try:
        # jobType contains the full SQL query. probably too much for the analysis
        # job_info['jobType'] = find(find(jlist, 'type', 'JOB_SUBMITTED')['event'], 'workflowName')

        job_info['finishTime'] = find(find(jlist, 'type', 'JOB_FINISHED')['event'], 'finishTime')
    except TypeError:
        raise ValueError('Job failed')

    jobcounters = find(find(find(jlist, 'type', 'JOB_FINISHED')['event'],
                            'totalCounters')['groups'],
                       'name', 'org.apache.hadoop.mapreduce.JobCounter')
    try:
        job_info['frameworkMs'] = find(find(find(jlist, 'type', 'JOB_FINISHED')['event'],
                                            'displayName', 'Map-Reduce Framework')['counts'],
                                       'name', 'CPU_MILLISECONDS')['value']
    except TypeError:
        job_info['frameworkMs'] = 0
    try:
        job_info['mapSlotMs'] = find(jobcounters['counts'], u'name', u'MILLIS_MAPS')['value']
    except TypeError:
        job_info['mapSlotMs'] = 0
    try:
        job_info['reduceSlotMs'] = find(jobcounters['counts'], u'name', u'MILLIS_REDUCES')['value']
    except TypeError:
        job_info['reduceSlotMs'] = 0

    job_info['dt'] = datetime.datetime.fromtimestamp(
        job_info['submitTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')

    # shorten names by removing any qualifiers after brackets
    try:
        job_info['jobName'] = job_info['jobName'][:job_info['jobName'].index('(')]
    except ValueError:
        pass
    return job_info


def main(basename):
    ARCHIVE = os.path.join(settings.WORKDIR, basename + '.tgz')
    if False:
        HISTORY_FILE = os.path.join(settings.WORKDIR,
                                    'history-{timestamp}.csv'.format(
                                        timestamp=datetime.datetime.now().strftime('%Y-%m-%d')
                                    ))
    else:
        HISTORY_FILE = os.path.join(settings.WORKDIR, basename + '.csv')

    print 'HISTORY_FILE: ', HISTORY_FILE

    # with gzip.open(HISTORY_FILE, 'wb') as csvfile:
    with open(HISTORY_FILE, 'wb') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=JOB_INFO_FIELDS, dialect='excel')

        writer.writeheader()

        # for f in history_files():
        for contents in history_tar(ARCHIVE):
            try:
                job_info = read_jhist_file(contents)
                writer.writerow(job_info)
            except ValueError:
                # ignore failed jobs
                pass

    # upload_to_s3(HISTORY_FILE)
    # print 'Job histories uploaded to ', HISTORY_FILE


if __name__ == "__main__":
    main(sys.argv[1])
