from __future__ import print_function

import os
import argparse
import json
import docker
import shutil
import tempfile
import checksumdir
 
def build_python(dockefile):
    dockerfile.append("RUN yum install -y python")
    dockerfile.append("RUN curl https://bootstrap.pypa.io/get-pip.py | python")

def import_repo(repo_source,target):
    print("importing repo",repo_source,"to",target)
    if os.path.isdir(repo_source):
        shutil.copytree(repo_source, target)
    else:
        raise NotImplementedError

    return checksumdir.dirhash(target)

def prepare_image(repo_source,from_image, service=True):
    tempdir=tempfile.mkdtemp()

    rel_repo_path="repo"
    repo_path=os.path.join(tempdir,rel_repo_path)

    repo_hash=import_repo(repo_source,repo_path)
    
    dockerfile=[]

    dockerfile.append("FROM {}".format(from_image))
    dockerfile.append("ARG REPO_PATH=./{}".format(rel_repo_path))
    dockerfile.append("ADD $REPO_PATH/requirements.txt /requirements.txt")
    dockerfile.append("RUN pip install -r /requirements.txt".format(repo_hash))
    dockerfile.append("ADD $REPO_PATH /repo")
    dockerfile.append("RUN touch /repo-hash-{}; pip install -r /repo/requirements.txt".format(repo_hash))
    dockerfile.append("RUN useradd -ms /bin/bash oda")
    dockerfile.append("USER oda")
    dockerfile.append("ARG nb2workflow_revision".format(from_image))
    dockerfile.append("RUN git clone https://github.com/volodymyrss/nb2workflow.git /nb2workflow; cd /nb2workflow; git reset --hard $nb2workflow_revision; pip install -r requirements.txt; pip install .; rm -rf /nb2workflow") 
    dockerfile.append("WORKDIR /workdir")

    if service:
        dockerfile.append("ENTRYPOINT nb2service /repo/ --host 0.0.0.0" )
    else:
        dockerfile.append("ENTRYPOINT nbrun /repo/" )

    open(os.path.join(tempdir,"Dockerfile"),"w").write(("\n".join(dockerfile))+"\n")

    return tempdir


def build_image(tempdir,tag_image,nb2workflow_revision):
    cli=docker.from_env()
    
    print("-- building image, tagging as",tag_image)
    r=cli.api.build(
                path=tempdir,
                tag=tag_image,
                quiet=False,
                buildargs=dict(nb2workflow_revision=nb2workflow_revision),
               # stream=True,
                rm=True,
            )
    for k in r:
        try:
            print(json.loads(k)['stream'].strip())
        except:
            print(k)

    return True

def main():

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('repo', metavar='repo', type=str)
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--build', action='store_true')
    parser.add_argument('--job', action='store_true')
    parser.add_argument('--name', metavar='TAG', type=str, default="nb2worker")
    parser.add_argument('--from-image', metavar='FROM IMAGE', type=str, default="python:3.6")
    parser.add_argument('--tag-image', metavar='TAG', type=str, default="")
    parser.add_argument('--host', metavar='host', type=str, default="127.0.0.1")
    parser.add_argument('--port', metavar='port', type=int, default=9191)
    parser.add_argument('--nb2wrev', metavar='TAG', type=str, default="master")
    parser.add_argument('--volume', metavar='mount:mount', type=str, nargs="*")
    parser.add_argument('--store-dockerfile', metavar='location', type=str, default=None)


    args = parser.parse_args()

    repo_path=args.repo
    tag_image=args.tag_image

    if args.tag_image == "":
        tag_image=os.path.basename(os.path.abspath(repo_path))

    tempdir=prepare_image(repo_path,args.from_image, service=not args.job)

    if args.store_dockerfile:
        shutil.copy(os.path.join(tempdir,"Dockerfile"),args.store_dockerfile)
        print("stored Dockerfile as",args.store_dockerfile)

    if args.build:
        build_result=build_image(tempdir,tag_image,args.nb2wrev)

        if build_result is None:
            raise Exception("failed to build")

        print("built:",build_result)

        if args.run:
            if not args.job:
                print("running",tag_image,"service on",args.port)
                cli=docker.from_env()
                c=cli.containers.run(
                    tag_image,
                    user=os.getuid(),
                    ports={ 9191: (args.host, args.port) },
                    name=args.name,
                    detach=True,
                    volumes=dict([
                        (os.getcwd(),{"bind":"/workdir","mode":"rw"}),
                    ]+[v.split(":",1) for v in args.volume]),
                )

                for r in c.attach(stream=True):
                    print(c,r.strip())
            else:
                print("running",tag_image)
                cli=docker.from_env()
                c=cli.containers.run(
                    tag_image,
                    user=os.getuid(),
                    name=args.name,
                    detach=True,
                    volumes=dict([
                        (os.getcwd(),{"bind":"/workdir","mode":"rw"}),
                    ]+[v.split(":",1) for v in args.volume]),
                    entrypoint=["nbrun", "/repo/*ipynb"],
                )

                for r in c.attach(stream=True):
                    print(c,r.strip())

if __name__=="__main__":
    main()
