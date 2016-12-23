#! /usr/bin/env bash
lib=lib/*

cpath='';
for jar in $lib;
do
        cpath+=":"$jar
done;
#echo "------===$cpath===-----"

cpath2=.:`echo /opt/kafka_spark_hbase_demo-1.0/lib/*.jar | tr ' ' ':'`
#echo "---->>$cpath2<<-----"

cpath3='/opt/kafka_spark_hbase_demo-1.0/lib/'
#echo "----++++++$cpath2+++-----"

cpath4="/usr/hdp/current/spark-client/bin/spark-submit --driver-class-path $cpath2 --class com.wankun.logcount.spark.LogStream /opt/kafka_spark_hbase_demo-1.0/lib/kafka_spark_hbase_demo-1.0.jar"

`$cpath4`

echo "---------%%%%$cpath4%%%%----------"
#java  -classpath $CLASSPATH$cpath com.wankun.logcount.spark.LogStream