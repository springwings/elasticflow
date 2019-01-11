#!/bin/bash
ps aux | grep java |grep river | awk '{print $2}' | xargs kill -15
ps aux | grep java |grep river | awk '{print $2}'

