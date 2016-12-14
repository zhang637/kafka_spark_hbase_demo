#!/usr/bin/env bash
lib=lib/*

cpath='';
for jar in $lib;
do
	cpath+=":"$jar
done;

/usr/hdp/current/spark-client/bin/spark-submit --class com.wankun.logcount.spark.LogStream ./lib/kafka_spark_hbase_demo-1.0.jar --jars $cpath