#!/bin/bash

#This script will run at midnight local time (MDT) each day  to build the trends for the day that just completed

path_to_trendulo_ingest_jar=
trending_class=trendulo.ingest.mapreduce.SimpleTrendDetectionWholeRow
instance=trendulo
zookeepers=localhost
tweets_table=tweets
trends_table=trends
user=root
password=

today=`date -d "-1 day" +"%Y%m%d"`
yesterday=`date -d "-2 day" +"%Y%m%d"`

command="${ACCUMULO_HOME}/bin/tool.sh $path_to_trendulo_ingest_jar  $trending_class $instance $zookeepers $tweets_table $trends DAY $yesterday $today -u $user -p $password"
echo $command;
