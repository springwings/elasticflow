#!/bin/bash
#OPTS
PROC_NAME=elasticflow
JAVA_OPTS="$JAVA_OPTS -server -Xms4g -Xmx4g -Xmn512M -XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=8 -XX:+HeapDumpOnOutOfMemoryError"
echo "$PROC_NAME wait to start..." 
ProcNumber=`ps -ef |grep -w $PROC_NAME|grep -v grep|wc -l`
if [ $ProcNumber -le 0 ];then
   java ${JAVA_OPTS} -Dnodeid=16 -Dconfig=file:/work/EF/slaveconfig -jar -Dplugin=/work/EF/plugin -jar target/elasticflow.jar  
   echo "$PROC_NAME start success!"
   ps aux | grep java |grep $PROC_NAME | awk '{print $2}'
else
   echo "WARNING $PROC_NAME is running.."
fi
