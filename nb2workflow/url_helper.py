from urllib.parse import urlparse


def is_mmoda_url(url):
    parsed_url = urlparse(url)
    if 'mmoda' in parsed_url.path or 'dispatch-data' in parsed_url.path or '_is_mmoda_url' in parsed_url.query:
        return True
    return False
