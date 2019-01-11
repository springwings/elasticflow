#!/bin/bash
#OPTS
JAVA_OPTS="$JAVA_OPTS -server -Xms2g -Xmx2g -Xmn512M -XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=8 -XX:+HeapDumpOnOutOfMemoryError"

nohup ${JAVA_HOME}/bin/java ${JAVA_OPTS} -Dconfig=/opt/config -jar  rivers.jar > nohup.txt 2>&1 &
