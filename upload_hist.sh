#!/usr/bin/env bash
HIST_DIR=/var/log/hadoop/history/2016/04
ARCHIVE='/home/hadoop/datapipeline/work/temp/hadoop-history-'`date +%F`'.tgz'
S3_STORE='s3://bi.dev.kristian/metrics/hadoop-jobs/'

tar -czf $ARCHIVE --exclude='*.xml' $HIST_DIR
aws s3 mv $ARCHIVE $S3_STORE
