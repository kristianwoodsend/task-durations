#!/bin/bash
F=$1

echo First, run upload_hist.sh on scheduler.

cd ~/src/modules/data-pipeline/
aws s3 cp s3://bi.dev.kristian/metrics/hadoop-jobs/$F.tgz ./work/temp/.
python -m metrics.hadoop_hist $F

echo Need to enter latest csv files into R code manually
R --slave -f hadoop_history.R
