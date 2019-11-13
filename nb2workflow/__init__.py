import pkg_resources

name = "nb2workflow"

def version():
    v = pkg_resources.get_distribution("nb2workflow").version
    print(v)

    return v

