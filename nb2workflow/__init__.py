import pkg_resources

name = "nb2workflow"

def version(print_it=True):
    v = pkg_resources.get_distribution("nb2workflow").version
    if print_it:
        print(v)

    return v

