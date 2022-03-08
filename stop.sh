#!/bin/bash
PROC_NAME=elasticflow
echo "$PROC_NAME wait to stop..." 
ps aux | grep java |grep $PROC_NAME | awk '{print $2}' | xargs kill -15
ps aux | grep java |grep $PROC_NAME | awk '{print $2}'

sleep 2 #wait kill port
ProcNumber=`ps -ef |grep -w $PROC_NAME|grep -v grep|wc -l`
while [ $ProcNumber -gt 0 ]
 do
   ProcNumber=`ps -ef |grep -w $PROC_NAME|grep -v grep|wc -l`   
 done 
echo "$PROC_NAME stop success!"

