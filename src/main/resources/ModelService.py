# -*- coding: utf-8 -*-
# python 3.6
"""
model service interface

Author: chengwen
Modifier:
date:   2022/03/26 11:21

"""
            
import socket
import entrance
import sys


HOST = '127.0.0.1' 
_SK = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
_SK.bind((HOST,int(sys.argv[1])))

while True:
    try:
        _SK.listen()
        conn,addr = _SK.accept()
        data = conn.recv(1024)
        data = bytes.decode(data)
        result = entrance.predict(data)
        conn.sendall(result)     
        conn.close()    
    except Exception:
        pass


