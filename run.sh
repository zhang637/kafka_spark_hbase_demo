#!/usr/bin/env bash

lib=lib/*

cpath='';

for jar in $lib;
do
	cpath+=":"$jar
done;

java -Dlogback.configurationFile=./conf/logback.xml -classpath .:$cpath com.wankun.logcount.kafka.TailService "$@"