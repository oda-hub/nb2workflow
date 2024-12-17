from importlib.metadata import version as pkg_ver

name = "nb2workflow"

def version(print_it=True):
    v = pkg_ver("nb2workflow")
    if print_it:
        print(v)

    return v

