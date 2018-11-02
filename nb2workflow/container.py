import os
import argparse
import docker
import shutil
import tempfile
 
def build_python(dockefile):
    dockerfile.append("RUN yum install -y python")
    dockerfile.append("RUN curl https://bootstrap.pypa.io/get-pip.py | python")

def import_repo(repo_source,target):
    print("importing repo",repo_source,"to",target)
    if os.path.isdir(repo_source):
        shutil.copytree(repo_source, target)
    else:
        raise NotImplemented

def build_image(repo_source,from_image,tag_image):
    tempdir=tempfile.mkdtemp()

    rel_repo_path="repo"
    repo_path=os.path.join(tempdir,rel_repo_path)

    import_repo(repo_source,repo_path)
    
    dockerfile=[]

    dockerfile.append("FROM {}".format(from_image))
    dockerfile.append("RUN git clone https://github.com/volodymyrss/nb2workflow.git /nb2workflow; cd /nb2workflow; pip install -r requirements.txt; pip install .")
    dockerfile.append("ADD ./{} /repo".format(rel_repo_path))
    dockerfile.append("RUN pip install -r /repo/requirements.txt".format(rel_repo_path))

    open(os.path.join(tempdir,"Dockerfile"),"w").write(("\n".join(dockerfile))+"\n")

    cli=docker.from_env()
    
    print("building image, tagging as",tag_image)
    return cli.images.build(
                    path=tempdir,
                    tag=tag_image,
                    quiet=False,
                )

def main():

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('repo', metavar='repo', type=str)
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--from-image', metavar='FROM IMAGE', type=str, default="python:2.7")
    parser.add_argument('--tag-image', metavar='TAG', type=str, default="")
    #parser.add_argument('--host', metavar='host', type=str, default="127.0.0.1")
    #parser.add_argument('--port', metavar='port', type=int, default=9191)

    args = parser.parse_args()

    repo_path=args.repo
    tag_image=args.tag_image

    if args.tag_image == "":
        tag_image=os.path.basename(os.path.abspath(repo_path))

    build_result=build_image(repo_path,args.from_image,tag_image)

    if build_result is None:
        raise Exception("failed to build")

    if args.run:
        cli=docker.from_env()
        cli.containers.run(
            tag_image,
            user=os.getuid(),
            ports={ 9191:9191 },
            command="nb2service /repo/ --host 0.0.0.0", 
        )
