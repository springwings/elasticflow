#!/bin/bash
#OPTS
JAVA_OPTS="$JAVA_OPTS -server -Xms4g -Xmx4g -Xmn512M -XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=8 -XX:+HeapDumpOnOutOfMemoryError"

nohup ${JAVA_HOME}/bin/java ${JAVA_OPTS} -Dnodeid=1 -Dconfig=file:/opt/config -jar -Dplugin=/opt/plugin -jar ElasticFlow.jar >/dev/null 2>&1 & 
