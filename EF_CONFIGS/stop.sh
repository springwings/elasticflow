#!/bin/bash

PROC_NAME="elasticflow"
echo "$PROC_NAME wait to stop..."

pkill -15 -f "java.*$PROC_NAME"

while pkill -0 -f "java.*$PROC_NAME" > /dev/null; do
    sleep 1
done

echo "$PROC_NAME stop success!"
