import re
import consul

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

    cc = consul.Consul(host = host, scheme = scheme, port = port)

    logger.debug("found services: %s",cc.agent.services())
    
    cc.agent.service.register(name, address = service_host, port = service_port, tags = ["nb2service"])

