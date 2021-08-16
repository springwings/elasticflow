#!/bin/bash
PROC_NAME=elasticflow
ps aux | grep java |grep $PROC_NAME | awk '{print $2}' | xargs kill -15
ps aux | grep java |grep $PROC_NAME | awk '{print $2}'
sleep 2 #wait kill port
