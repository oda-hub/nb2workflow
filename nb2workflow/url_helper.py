from urllib.parse import urlparse


def is_mmoda_url(url):
    parsed_url = urlparse(url)
    if 'mmoda' in parsed_url.path:
        return True
    return False
