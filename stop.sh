#!/bin/bash
PROC_NAME=elasticflow
echo "$PROC_NAME start stop..." 
ps aux | grep java |grep $PROC_NAME | awk '{print $2}' | xargs kill -15
ps aux | grep java |grep $PROC_NAME | awk '{print $2}'
sleep 2 #wait kill port
echo "$PROC_NAME stop success!" 
