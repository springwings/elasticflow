# -*- coding: utf-8 -*-
# python 3.6
"""
model service interface

Author: chengwen
Modifier:
date:   2022/03/26 11:21

"""
            
import socket

HOST = '127.0.0.1' 
_SK = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
_SK.bind((HOST,PORT))

while True:
    try:
        s.listen()
        conn,addr = s.accept()
        data = conn.recv(1024)
        data = bytes.decode(data)
        result = predict(data)
        conn.sendall(result)     
        conn.close()    
    except Exception:
        pass


