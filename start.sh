#!/bin/bash
#OPTS
PROC_NAME=elasticflow
JAVA_OPTS="$JAVA_OPTS -server -Xms4g -Xmx4g -Xmn512M -XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=8 -XX:+HeapDumpOnOutOfMemoryError"

ProcNumber=`ps -ef |grep -w $PROC_NAME|grep -v grep|wc -l`
if [ $ProcNumber -le 0 ];then
   nohup ${JAVA_HOME}/bin/java ${JAVA_OPTS} -Dnodeid=1 -Dconfig=file:/opt/config -jar -Dplugin=/opt/plugin -jar elasticflow.jar >/dev/null 2>&1 &
   echo "$PROC_NAME start success!" 
else
   echo "$PROC_NAME is running.."
fi
