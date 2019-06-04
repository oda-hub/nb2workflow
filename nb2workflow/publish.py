import re

import logging

logger = logging.getLogger('nb2workflow.publish')

def publish(upstream_url, name, service_host, service_port):
    r = re.match("(.*?)://(.*?)(?:$|:)(\d*)",upstream_url)
    if r:
        scheme, host, port = r.groups()
    else:
        r = re.match("(.*?)(?:$|:)(\d*)",upstream_url)
        scheme = "http"
        host, port = r.groups()
    
    if port == "":
        port = 8500
    else:
        port = int(port)


