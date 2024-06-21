from urllib.parse import urlparse


def is_mmoda_url(url):
    parsed_url = urlparse(url)
    if 'mmoda' in parsed_url.path or 'dispatch-data' in parsed_url.path or '_is_mmoda_url' in parsed_url.query:
        return True
    return False


def serialize_workflow_exception(e):
    try:
        return dict(
                    ename = e[0].ename,
                    evalue = e[0].evalue,
                    edump = e[1][0],
                )
    except (TypeError, AttributeError):
        return dict(
                    ename = repr(e),
                    evalue = "",
                    edump = repr(e)
                )
