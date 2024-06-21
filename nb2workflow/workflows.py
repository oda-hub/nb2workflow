import json
import os
import requests
import time
from collections import OrderedDict

from nb2workflow.helpers import serialize_workflow_exception
from . import logstash
from .sentry import sentry

from diskcache import Cache

from nb2workflow import nbadapter

cache = Cache('.nb2workflow/cache')
enable_cache = False

logstasher = logstash.LogStasher()

class WorkflowException(Exception):
    pass

def reroute(router, *args, **kwargs):
    workflow_routes = dict([ r.split("=") for r in os.environ.get('WORKFLOW_ROUTES','').split(",") if len(r.split("=")) == 2 ])

    workflow = args[0]

    if workflow in workflow_routes:
        r_w = workflow_routes[workflow]
        return r_w.split(":") + args, kwargs
    
    if workflow in os.environ.get('STAGING_WORKFLOWS','').split(','):
        return router+"-staging", args, kwargs

    return router, args, kwargs

def evaluate(router, *args, **kwargs):
    key = json.dumps((router, args, OrderedDict(sorted(kwargs.items()))))

    ntries = kwargs.pop('_ntries', 30)
    async_request = kwargs.pop('_async_request', True)
    cached = kwargs.pop('_cached', True)

    print("async_request is not used here, but is set to", async_request)


    logstasher.set_context(dict(router=router, args=args, kwargs=kwargs))
    logstasher.log(dict(event='starting'))

    if cached and enable_cache and key in cache:
        v = cache.get(key)
        print("restored from cache, key:", key)
        print("restored from cache, value:", v)

        if v == {} or v is None:
            print("this value is empty, regenerate")
        else:
            return v

    print("before routing", router, args, kwargs)
    router, args, kwargs = reroute(router, *args, **kwargs)
    print("after routing", router, args, kwargs)

    if router == "localfile":
        location = args[0]
        args = args[1:]

        nba = nbadapter.NotebookAdapter(location+"/%s.ipynb"%args[0])

        # unused args

        params = kwargs

        print("calling",params)

        exceptions = nba.execute(params,
                    log_output=True,
                    progress_bar=False)

        output = nba.extract_output()

        result = dict(output = output, exceptions = [serialize_workflow_exception(e) for e in exceptions])

    elif router.startswith("odahub") or router.startswith("host"):
        workflow = args[0]

        if router == "odahub-staging":
            url_template = f"https://oda-workflows-{workflow}-staging.odahub.io/api/v1.0/get/" + "{}"

        if router == "odahub":
            url_template = f"https://oda-workflows-{workflow}.odahub.io/api/v1.0/get/" + "{}"
    
        if router == "host":
            url_template = args[0]+"/api/v1.0/get/{}"

        url = url_template.format(*args[1:])
        print("url:",url)

        ntries = ntries
        while ntries > 0:
            try:
                print("towards",ntries,url,kwargs)
                c=requests.get(
                    url=url,
                    params=kwargs,
                    auth=requests.auth.HTTPBasicAuth("cdci", open("/cdci-resources/reproducible").read().strip())
                )
                print("decoding",c.text)

                try:
                    result = c.json()
                except Exception as ed:
                    print("problem decoding:", repr(ed))
                    print("raw output:",c.text)
                    logstasher.log(dict(event='failed to decode output',raw_output=c.text, exception=repr(ed)))
                    raise


                if 'output' in result and 'workflow_status' in result['output']:
                    if result['output']['workflow_status'] != "done": # bad
                        print("waiting for async workflow")
                        time.sleep(5)

                        ntries -= 1
                        continue

                break

            except Exception as e:
                print("problem from service", repr(e))

                logstasher.log(dict(event='problem evaluating',exception=repr(e)))
                
                if ntries <= 1:
                    sentry.capture_exception(e)
                    raise

                time.sleep(5)

                ntries -= 1

        if 'output' not in result:
            result = dict(output=result)

                #raise
    else:
        raise NotImplementedError


    logstasher.log(dict(event='done'))

    cache.set(key, result)
    print("stored to cache", key)

    return result

