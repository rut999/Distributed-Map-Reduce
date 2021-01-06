# -*- coding: utf-8 -*-
"""
Created on Sat Oct 10 17:01:52 2020
Title: Assignment 2 - MapReduce - KVServer
@author: rparvat
"""

import rpyc
from rpyc.utils.server import ThreadedServer
import configparser
import os
import threading
# import platform
# import signal
# import ctypes

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

#creating a global lock object
lock = threading.Lock()

class KeyValueService(rpyc.Service):
    
    def __init__(self):
        self.final_filename = 'final_kv_store.txt' 
        
    def on_connect(self, conn):
        print(f"{conn._channel.stream.sock.getpeername()} connected")
        pass

    def on_disconnect(self, conn):
        print(f"disconnected")
        pass
        
    def exposed_set_data_chunk(self,data,num_reducers,task):
        if task == 'word_count':
            for i in range(len(data)):
                filename = "mapper_input"+str(i)+".txt"
                file_write = open(filename, 'w')
                file_write.write(str(data[i]))
                file_write.close()
        if task == 'inverted_index':
            filename = "file_mapping.txt"
            file_write = open(filename, 'w')
            file_write.write(str(data))
            file_write.close()
        
        for i in range(num_reducers):
            file = "reducer_input"+str(i)+".txt"
            fw = open(file,'w')
            fw.close()
            
        if os.path.exists(self.final_filename):
            os.remove(self.final_filename)
        open(self.final_filename, 'x')
            
    def exposed_get(self,filename):
        f = open(filename, "r")
        return f.read()
    
    def exposed_map_set(self,map_out):
        for i in range(len(map_out)):
            file = "reducer_input"+str(i)+".txt"
            output = open(file, 'a')
            for tup in map_out[i]:
                with lock:
                    output.write(str(tup[0])+','+str(tup[1]) + '\n')
            output.close()

    
    def exposed_red_set(self,data):
        output = open(self.final_filename, 'a')
        for tup in data:
            with lock:
                output.write(str(tup) + '\n')
        output.close()
        
            
def start_server(port):
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
    t = ThreadedServer(KeyValueService(), port = port, protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    try:
        t.start()
        print('KVServer started on port: ', port)
    except Exception:
        t.stop()
        
if __name__ == '__main__':
    
    #config parameters
    config = configparser.ConfigParser()
    config.read('config.ini')
    kvport = int(config['KVServer']['kvport'])
    start_server(kvport)