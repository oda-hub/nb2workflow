import pkg_resources

name = "nb2workflow"

def version():
    print(pkg_resources.get_distribution("nb2workflow").version)

