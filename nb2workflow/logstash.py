import socket
import json
import sys


class LogStasher:
    def __init__(self, url=None):
        if url is None:
            self.url = os.environ.get("LOGSTASH_ENTRYPOINT", open("/cdci-resources/logstash-entrypoint").read().strip())
        else:
            self.url = url

        self.context = {}

    def set_context(self, c):
        self.context = c
    
    def log(self, msg):
        HOST, PORT = self.url.split(":")
        PORT = int(PORT)

        msg = dict(list(self.context.items()) + list(msg.items()))

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except Exception as e:
            print("[ERROR] %s\n" % repr(e)) 
            

        try:
            sock.connect((HOST, PORT))
        except Exception as e:
            print("[ERROR] %s\n" % repr(e)) 

        sock.send(json.dumps(msg).encode())

        sock.close()
