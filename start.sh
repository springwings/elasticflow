#!/bin/bash
#OPTS
JAVA_OPTS="$JAVA_OPTS -server -Xms4g -Xmx4g -Xmn512M -XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=8 -XX:+HeapDumpOnOutOfMemoryError"

nohup ${JAVA_HOME}/bin/java ${JAVA_OPTS} -Dconfig=file:/opt/config -jar  ElasticFlow.jar > nohup.txt 2>&1 &
