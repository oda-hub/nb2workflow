import re
import logging
logger = logging.getLogger('nb2workflow.publish')

try:
    import consul
except:
    consul = None


def publish(upstream_url, name, service_host, service_port):
    if consul is None:
        return

    r = re.match(r"(.*?)://(.*?)(?:$|:)(\d*)",upstream_url)
    if r:
        scheme, host, port = r.groups()
    else:
        r = re.match(r"(.*?)(?:$|:)(\d*)",upstream_url)
        scheme = "http"
        host, port = r.groups()
    
    if port == "":
        port = 8500
    else:
        port = int(port)

    cc = consul.Consul(host = host, scheme = scheme, port = port)

    logger.debug("found services: %s",cc.agent.services())

    logger.debug("will publish as %s, %s",service_host, service_port)
    
    cc.agent.service.register(name, address = service_host, port = service_port, tags = ["nb2service", "traefik.protocol=https"])

