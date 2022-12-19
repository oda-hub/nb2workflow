import socket
import json
import sys
import os

import collections

def flatten(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        #print("k,v",k,v)
        new_key = str(parent_key) + sep + str(k) if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        elif isinstance(v, collections.Iterable) and not isinstance(v,str):
            items.extend(flatten(dict(enumerate(v)), new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


class LogStasher:
    def __init__(self, url=None):
        self.url = url

        if self.url is None:
            self.url = os.environ.get("LOGSTASH_ENTRYPOINT", None)
            
        self.context = {}

    def set_context(self, c):
        self.context = c
    
    def log(self, msg):
        if self.url is not None:
            HOST, PORT = self.url.split(":")
            PORT = int(PORT)

            msg = flatten(dict(list(self.context.items()) + list(msg.items())))


            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((HOST, PORT))
                sock.send(json.dumps(msg).encode())
                sock.close()
            except Exception as e:
                print("[ERROR] %s\n" % repr(e)) 

