from __future__ import print_function

import os
import sys
import argparse
import json
import docker
import shutil
import tempfile
import checksumdir
import subprocess

from nb2workflow import version
from nb2workflow.cwl import nb2cwl_container
from nb2workflow.nbadapter import find_notebooks

import logging

logger = logging.getLogger(__name__)


def build_python(dockerfile):
    dockerfile.append("RUN yum install -y python")
    dockerfile.append("RUN curl https://bootstrap.pypa.io/get-pip.py | python")


def import_repo(repo_source, target):
    print("importing repo", repo_source, "to", target)
    if os.path.isdir(repo_source):
        shutil.copytree(repo_source, target)
    else:
        raise NotImplementedError

    return checksumdir.dirhash(target)


def prepare_image(repo_source, from_image, service=True, nb2w_path=None, runprefix="", entrypoint=None):

    tempdir = tempfile.mkdtemp()

    rel_repo_path = "repo"
    repo_path = os.path.join(tempdir, rel_repo_path)

    repo_hash = import_repo(repo_source, repo_path)

    dockerfile = []

    dockerfile.append("FROM {}".format(from_image))
    dockerfile.append("ARG REPO_PATH=./{}".format(rel_repo_path))

    pipconf = os.path.join(repo_path, 'pip.conf')
    logger.info("using pipconf %s", pipconf)
    if os.path.exists(pipconf):
        dockerfile.append("ENV PIP_CONFIG_FILE=/etc/pip/pip.conf")
        dockerfile.append("ADD $REPO_PATH/pip.conf /etc/pip/pip.conf")

    dockerfile.append("ADD $REPO_PATH/requirements.txt /requirements.txt")
    dockerfile.append("ADD $REPO_PATH/environment.yml /environment.yml")
    dockerfile.append("RUN {} pip install --upgrade pip".format(runprefix))
   
    dockerfile.append(('RUN {} conda env update -f /environment.yml && '
                       'pip install -r /requirements.txt').format(runprefix))
 
    dockerfile.append("ADD $REPO_PATH /repo")
    dockerfile.append(
        "RUN {} touch /repo-hash-{}; pip install -r /repo/requirements.txt --upgrade".format(runprefix, repo_hash))
    dockerfile.append("RUN useradd -ms /bin/bash oda")

    if nb2w_path is None:
        dockerfile.append(
            "RUN {} pip install nb2workflow=={}".format(runprefix, version()))
    else:
        subprocess.check_output(
            ["git", "clone", nb2w_path, os.path.join(tempdir, "nb2workflow")])
        dockerfile.append("ADD ./nb2workflow /nb2workflow")
        dockerfile.append(
            "RUN {} cd /nb2workflow; pip install .".format(runprefix))

    dockerfile.append("USER oda")
    dockerfile.append("WORKDIR /workdir")

    if service:
        logger.info('service mode')
        dockerfile.append("ENTRYPOINT nb2service /repo/ --host 0.0.0.0")
    else:
        logger.info('not service mode')
        dockerfile.append('ENTRYPOINT []')
        #dockerfile.append('ENTRYPOINT ["bash"]' )
        #dockerfile.append('ENTRYPOINT ["nbrun", "/repo/"]' )

    open(os.path.join(tempdir, "Dockerfile"), "w").write(
        ("\n".join(dockerfile))+"\n")

    return tempdir


def build_image(tempdir, tag_image, nb2workflow_revision):
    cli = docker.from_env()

    print("-- building image, tagging as", tag_image)
    r = cli.api.build(
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
    parser.add_argument('--from-image', metavar='FROM IMAGE',
                        type=str, default="python:3.6")
    parser.add_argument('--tag-image', metavar='TAG', type=str, default="")
    parser.add_argument('--host', metavar='host',
                        type=str, default="127.0.0.1")
    parser.add_argument('--port', metavar='port', type=int, default=9191)
    parser.add_argument('--nb2wrev', metavar='TAG', type=str, default="master")
    parser.add_argument('--nb2wpath', metavar='PATH', type=str)
    parser.add_argument('--volume', metavar='mount:mount', type=str, nargs="*", default=[])
    parser.add_argument('--docker-run-prefix',  type=str, default="")
    parser.add_argument('--store-dockerfile',
                        metavar='location', type=str, default=None)
    parser.add_argument('--docker-command', type=str, default=None)
    parser.add_argument('--entrypoint', type=str, default=None)

    args = parser.parse_args()

    repo_path = args.repo
    tag_image = args.tag_image

    if tag_image.lower() != tag_image:
        print("\033[31mdocker does not accept tags with anything but lowercase!\033[0m")
        print(f"\033[31mthis is bad: {tag_image}\033[0m")
        sys.exit(1)

    if args.tag_image == "":
        tag_image = os.path.basename(os.path.abspath(repo_path))

    tempdir = prepare_image(
        repo_path, args.from_image,
        service=not args.job,
        nb2w_path=args.nb2wpath,
        runprefix=args.docker_run_prefix,
        entrypoint=args.entrypoint,
    )

    if args.store_dockerfile:
        shutil.copy(os.path.join(tempdir, "Dockerfile"), args.store_dockerfile)
        print("\033[31mstored Dockerfile as\033[0m", args.store_dockerfile)

    if args.build:
        build_result = build_image(tempdir, tag_image, args.nb2wrev)

        if build_result is None:
            raise Exception("failed to build")

        print("built:", build_result)

        if args.job:

            for n, nba in find_notebooks(repo_path).items():
                nb2cwl_container(tag_image, nba.notebook_fn,
                                 n+".cwl", command=args.docker_command)

        if args.run:
            if not args.job:
                print("running", tag_image, "service on", args.port)
                cli = docker.from_env()
                c = cli.containers.run(
                    tag_image,
                    user=os.getuid(),
                    ports={9191: (args.host, args.port)},
                    name=args.name,
                    detach=True,
                    volumes=dict([
                        (os.getcwd(), {"bind": "/workdir", "mode": "rw"}),
                    ]+[v.split(":", 1) for v in args.volume]),
                )

                for r in c.attach(stream=True):
                    print(c, r.strip())
            else:
                print("running", tag_image)
                cli = docker.from_env()
                c = cli.containers.run(
                    tag_image,
                    user=os.getuid(),
                    name=args.name,
                    detach=True,
                    volumes=dict([
                        (os.getcwd(), {"bind": "/workdir", "mode": "rw"}),
                    ]+[v.split(":", 1) for v in args.volume]),
                    entrypoint=["nbrun", "/repo/*ipynb"],
                )

                for r in c.attach(stream=True):
                    print(c, r.strip())


if __name__ == "__main__":
    main()
