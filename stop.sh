#!/bin/bash
ps aux | grep java |grep ElasticFlow | awk '{print $2}' | xargs kill -15
ps aux | grep java |grep ElasticFlow | awk '{print $2}'
sleep 2 #wait kill port
