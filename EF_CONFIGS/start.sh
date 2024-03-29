#!/bin/bash
#OPTS
PROC_NAME=elasticflow
JAVA_OPTS="$JAVA_OPTS -server -Xms4g -Xmx4g -Xmn512M -XX:ParallelGCThreads=8 -XX:+HeapDumpOnOutOfMemoryError"
echo "$PROC_NAME wait to start..." 

required_java_version="11"

java_version_output=$(java -version 2>&1)

java_version=$(echo "$java_version_output" | grep -oP '(?<=version ")[^"]+')

if [ -z "$java_version" ]; then
    echo "Unable to obtain Java version information, please ensure that Java is installed correctly。"
    exit 1
fi

if [ "$(printf "%02d%02d" $(echo "$java_version" | awk -F'.' '{print $1,$2}'))" -lt "$(printf "%02d%02d" "$required_java_version")" ]; then
    echo "The Java version is too low, please install Java $required_java_version or higher。"
    exit 1
fi

ProcNumber=`ps -ef |grep -w $PROC_NAME|grep -v grep|wc -l`
if [ $ProcNumber -le 0 ];then
   nanoseconds=$(expr $(date +%N) + 0)
   nohup java ${JAVA_OPTS} -Dnodeid=${nanoseconds} -Dconfig=/opt/EF -jar -Dplugin=/opt/EF/plugin -jar elasticflow.jar >/dev/null 2>&1 &
   sleep 1
   ProcNumber=`ps -ef |grep -w $PROC_NAME|grep -v grep|wc -l`
   if [ $ProcNumber -gt 0 ];then  
      pid=$(pgrep -f "$PROC_NAME")
      echo "$PROC_NAME start success! nodeID $nanoseconds, PID $pid"
   else
      echo "$PROC_NAME start failed!"
   fi
else
   echo "WARNING $PROC_NAME is running..."
fi
