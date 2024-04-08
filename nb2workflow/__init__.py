import pkg_resources
import os

name = "nb2workflow"
conf_dir = os.path.dirname(__file__)+'/config_dir'

def version():
    v = pkg_resources.get_distribution("nb2workflow").version
    print(v)

    return v

