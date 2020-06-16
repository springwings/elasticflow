#!/bin/bash
#OPTS
JAVA_OPTS="$JAVA_OPTS -server -Xms2g -Xmx2g -Xmn512M -XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=8 -XX:+HeapDumpOnOutOfMemoryError"

nohup ${JAVA_HOME}/bin/java ${JAVA_OPTS} -Dconfig=file:/opt/config -jar  ElasticFlow.jar > nohup.txt 2>&1 &
