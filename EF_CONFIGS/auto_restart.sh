#!/bin/bash
 
PROC_NAME=elasticflow
APP_COMMAND="sh restart.sh"
 
while true; do 
    ProcNumber=`ps -ef |grep -w $PROC_NAME|grep -v grep|wc -l`
    if [ $ProcNumber -gt 0 ];then
        sleep 60
    else 
        echo "$PROC_NAME application crashed. Restarting..."
        $APP_COMMAND &
        sleep 10   
    fi
done