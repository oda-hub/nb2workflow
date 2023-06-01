import argparse
import json
import re
import logging
import os
import pathlib
import re
import subprocess as sp
import tempfile
import time
import yaml
from .logging_setup import setup_logging
from . import version
from datetime import datetime, timezone
from textwrap import dedent
import uuid


logger = logging.getLogger(__name__)

default_config = {
    "config_schema_version": "0.1.0",
    "notebook_path": "", # or "notebooks"
    "extra_data": [],
    "use_repo_base_image": False,
    "filename_pattern": '.*', 
}


#TODO: probably want an option to really use the dir
def determine_origin(repo):
    if os.path.isdir(repo):
        return sp.check_output(
            ["git", "remote", "get-url", "origin"],
            cwd=repo).decode().strip()
    else:
        return repo

def build_container(git_origin, 
                    local=False, 
                    run_tests=True, 
                    registry="odahub", 
                    build_timestamp=False,
                    engine = "docker",
                    cleanup = False,
                    nb2wversion = version(),
                    **kwargs):
    if engine == "docker":
        return _build_with_docker(git_origin=git_origin,
                                 local=local,
                                 run_tests=run_tests,
                                 registry = registry,
                                 build_timestamp=build_timestamp,
                                 nb2wversion = nb2wversion)
    elif engine == 'kaniko':
        if run_tests == True:
            logger.warning("KANIKO builder doesn't support run_tests . Will switch off")
        return _build_with_kaniko(git_origin=git_origin,
                                 registry = registry,
                                 local = local,
                                 build_timestamp=build_timestamp,
                                 namespace = kwargs['namespace'],
                                 nb2wversion = nb2wversion
                                 )
    else:
        return NotImplementedError('Unknown container build engine: %s', engine)


def _nb2w_dockerfile_gen(context_dir, git_origin, source_from, meta, nb2wversion):
    try:
        with open(pathlib.Path(context_dir) / "Dockerfile", "r") as fd:
            dockerfile_content = fd.read()
            dockerfile_content += "\n"
    except FileNotFoundError:
        dockerfile_content = ""
    
    local_repo_path = pathlib.Path(context_dir) / "nb-repo"
    config_fn = local_repo_path / "mmoda.yaml"

    config = default_config.copy()
    if os.path.exists(config_fn):
        extra_config = yaml.safe_load(open(config_fn))
        logger.info("extra config from %s: %s", config_fn, extra_config)
        config.update(extra_config)
    else:
        logger.info("no extra config in %s", config_fn)
    logger.info("complete config: %s", config)

    notebook_fullpath_in_container = pathlib.Path('/repo') / (config['notebook_path'].strip("/"))
    logger.info("using notebook_fullpath_in_container: %s", notebook_fullpath_in_container)

    if not config['use_repo_base_image']: 
        dockerfile_content = "FROM python:3.9\n"
        
    if source_from == 'localdir':
        dockerfile_content += "COPY nb-repo/ /repo/\n"
    elif source_from == 'git':
        dockerfile_content += ("RUN apt-get install -y git && " 
                               "curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash && " 
                               "apt-get install -y git-lfs &&"
                               "git lfs install\n")
        dockerfile_content += f"RUN git clone {git_origin} repo\n"
    else:
        raise NotImplementedError('Unknown source code location %s', source_from)
    
    if not config['use_repo_base_image']:         
        dockerfile_content += "RUN pip install -r repo/requirements.txt\n"

    if nb2wversion.startswith('git+'):
        dockerfile_content += f"RUN pip install git+https://github.com/oda-hub/nb2workflow@{nb2wversion[4:]}#egg=nb2workflow[service]\n"
    else:
        dockerfile_content += f"RUN pip install nb2workflow[service]=={nb2wversion}\n"
                    
    dockerfile_content += dedent(f"""       
        ENV ODA_WORKFLOW_VERSION="{meta['descr']}"
        ENV ODA_WORKFLOW_LAST_AUTHOR="{meta['author']}"
        ENV ODA_WORKFLOW_LAST_CHANGED="{meta['last_change_time']}"
        ENV ODA_WORKFLOW_NOTEBOOK_PATH="{notebook_fullpath_in_container}"
        ENV ODA_WORKFLOW_FILENAME_PATTERN="{config['filename_pattern']}"
        
        RUN curl -o /usr/bin/jq -L https://github.com/stedolan/jq/releases/download/jq-1.5/jq-linux64; \
            chmod +x /usr/bin/jq
        RUN for nn in $ODA_WORKFLOW_NOTEBOOK_PATH/*.ipynb; do mv $nn $nn-tmp; \
            jq '.metadata.kernelspec.name |= "python3"' $nn-tmp > $nn ; rm $nn-tmp ; done
        
        ENTRYPOINT nb2service --debug $ODA_WORKFLOW_NOTEBOOK_PATH --pattern "$ODA_WORKFLOW_FILENAME_PATTERN" --host 0.0.0.0 --port 8000 | cut -c1-500
        """)
    
    with open(pathlib.Path(context_dir) / "Dockerfile", "w") as fd:
        fd.write(dockerfile_content)
    
    return dockerfile_content

def _build_with_kaniko(git_origin,  
                      registry="odahub", 
                      local=False,
                      build_timestamp=False,
                      namespace="oda-staging",
                      cleanup = True,
                      nb2wversion = version()):
    
    #secret should be created beforehand https://github.com/GoogleContainerTools/kaniko#pushing-to-docker-hub
       
    container_metadata = _build_with_docker(git_origin=git_origin,
                                            registry=registry,
                                            build_timestamp=build_timestamp,
                                            dry_run=True,
                                            source_from='git',
                                            nb2wversion = nb2wversion)
    
    dockerfile_content = container_metadata['dockerfile_content']
    
    
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(pathlib.Path(tmpdir) / "Dockerfile", "w") as fd:
            fd.write(dockerfile_content)
        
        suffix = pathlib.Path(tmpdir).name.lower().replace('_', '-')
        
        sp.check_call([
            "kubectl",
            "create",
            "configmap",
            "-n", namespace,
            f"nb2w-dockerfile-{suffix}",
            "--from-file=Dockerfile=Dockerfile"
        ], cwd = tmpdir)
        
        dest = '--no-push' if local else f'--destination={container_metadata["image"]}'
        with open(pathlib.Path(tmpdir) / "buildjob.yaml", "w") as fd:
            fd.write(dedent(f"""\
                apiVersion: batch/v1
                kind: Job
                metadata:
                  name: kaniko-build-{suffix}
                  namespace: {namespace}
                spec:
                  template:
                    spec:
                      containers:
                      - name: kaniko-build
                        image: gcr.io/kaniko-project/executor:v1.9.2
                        imagePullPolicy: IfNotPresent
                        args:
                        - "--dockerfile=/tmp/build/Dockerfile"
                        - "--context=dir:///tmp/build"
                        - "{dest}"
                          
                        volumeMounts:
                        - name: dockerfile
                          mountPath: /tmp/build/Dockerfile
                          subPath: Dockerfile
                        - name: kaniko-secret
                          mountPath: /kaniko/.docker/config.json
                          subPath: config.json
                      volumes:
                      - name: dockerfile
                        configMap:
                          name: nb2w-dockerfile-{suffix}
                      - name: kaniko-secret
                        secret:
                          secretName: kaniko-secret
                      restartPolicy: Never
                """))
        
        sp.check_call([
            "kubectl",
            "create",
            "-f",
            "buildjob.yaml"
        ], cwd = tmpdir)
        
        sp.check_call([
            "kubectl",
            "-n",
            f"{namespace}",
            "wait",
            "--for=condition=complete",
            "--timeout=10m",
            f"job/kaniko-build-{suffix}"
        ])
        
        if cleanup:
            sp.check_call([
                "kubectl",
                "-n",
                f"{namespace}",
                "delete",
                f"job/kaniko-build-{suffix}"
            ])
            
            sp.check_call([
                "kubectl",
                "-n",
                f"{namespace}",
                "delete",
                "configmap",
                f"nb2w-dockerfile-{suffix}"
            ])
        
        return container_metadata


def _build_with_docker(git_origin, 
                    local=False, 
                    run_tests=True, 
                    registry="odahub", 
                    build_timestamp=False,
                    dry_run = False,
                    source_from = 'localdir',
                    cleanup = False,
                    nb2wversion = version()):
    if cleanup:
        logger.warning('Post-build cleanup is not implemented for docker builds')
    
    git_origin = determine_origin(git_origin)

    with tempfile.TemporaryDirectory() as tmpdir:        
        sp.check_call(# cli is more stable than python API
            ["git", "clone", git_origin, "nb-repo"],
            cwd=tmpdir)

        local_repo_path = pathlib.Path(tmpdir) / "nb-repo"

        meta = {}
        meta['descr'] = sp.check_output( # cli is more stable than python API
                            ["git", "describe", "--always", "--tags"],
                            cwd=local_repo_path ).decode().strip()
        
        meta['author'] = sp.check_output( 
                            ["git", "log", "-1", "--pretty=format:'%an <%ae>'"], # could use all authors too, but it's inside anyway
                            cwd=local_repo_path ).decode().strip()
            
        meta['last_change_time'] = sp.check_output( 
                                    ["git", "log", "-1", "--pretty=format:'%ai'"], # could use all authors too, but it's inside anyway
                                    cwd=local_repo_path ).decode().strip()

        dockerfile_content = _nb2w_dockerfile_gen(tmpdir, git_origin, source_from, meta, nb2wversion)

        ts = '-' + time.strftime(r'%y%m%d%H%M%S') if build_timestamp else ''
        image = f"{registry}/nb-{pathlib.Path(git_origin).name}:{meta['descr']}-nb2w{nb2wversion.replace('git+', '')}{ts}"

        if not dry_run:
            sp.check_call( # cli is more stable than python API
                ["docker", "build", ".", "-t", image],
                cwd=tmpdir)     

        if run_tests and not dry_run: 
            # TODO: run tests too
            # TODO: probably better to move this to deploy
            out = sp.check_output(
                    ["docker", "run", '--rm', '--entrypoint', 'bash', image, '-c', 
                     ('pip install nb2workflow[rdf,mmoda,service] --upgrade;'
                      'for a in $(ls $ODA_WORKFLOW_NOTEBOOK_PATH/*ipynb | grep -v test_); do'
                      '  nbinspect --machine-readable $a;'
                      '  nbrun --machine-readable $a;'
                      'done')
                     ],
                    cwd=tmpdir)
            # dispatcher signature is currently optional. 
            # Will be empty if dipsatcher plugin is not installed in the container
            workflow_dispatcher_signature = re.search(rb"^WORKFLOW-DISPATCHER-SIGNATURE: (.*?)$", out, re.M)
            if workflow_dispatcher_signature is not None:
                workflow_dispatcher_signature = json.loads(workflow_dispatcher_signature.group(1).decode())
            workflow_nb_signature = json.loads(re.search(rb"^WORKFLOW-NB-SIGNATURE: (.*?)$", out, re.M).group(1).decode())
        else:
            workflow_dispatcher_signature = None
            workflow_nb_signature = None
        
    if not local and not dry_run: 
        sp.check_call( # cli is more stable than python API
            ["docker", "push", image])
    
    return {"descr": meta['descr'],
            "image": image,
            "author": meta['author'],
            "last_change_time": meta['last_change_time'],
            "workflow_dispatcher_signature": workflow_dispatcher_signature,
            "workflow_nb_signature": workflow_nb_signature,
            "dockerfile_content": dockerfile_content}


def deploy(git_origin, 
           deployment_base_name, 
           namespace="oda-staging", 
           local=False, 
           run_tests=True, 
           check_live=True, 
           registry="odahub", 
           check_live_through = "oda-dispatcher",
           build_engine = 'docker',
           build_timestamp = False,
           cleanup = False,
           nb2wversion = version()):
    
    container = build_container(git_origin, 
                                local=local, 
                                run_tests=run_tests, 
                                registry=registry, 
                                engine=build_engine, 
                                namespace=namespace,
                                build_timestamp=build_timestamp,
                                cleanup = cleanup,
                                nb2wversion = nb2wversion)
    
    if local:
        sp.check_call( # cli is more stable than python API
            ["docker", "run", '-p', '8000:8000', container['image']])
    else:
        deployment_name = deployment_base_name + "-backend"
        try:
            sp.check_call(
                ["kubectl", "patch", "deployment", deployment_name, "-n", namespace,
                "--type", "merge",
                "-p", 
                json.dumps(
                    {"spec":{"template":{"spec":{
                        "containers":[
                            {"name": deployment_name, "image": container['image']}
                        ]}}}})
                ]
            )
        except sp.CalledProcessError:
            sp.check_call(
                ["kubectl", "create", "deployment", deployment_name, "-n", namespace, "--image=" + container['image']]
            )
            sp.check_call(
                ["kubectl", "expose", "deployment", deployment_name, "--name", deployment_name, 
                "--port", "8000", "-n", namespace]
            )
        
        finally:                    
            sp.check_call(
                ["kubectl", "patch", "deployment", deployment_name, "-n", namespace,
                "--type", "strategic",
                "-p", 
                json.dumps(
                    {"spec":{"template":{"spec":{
                        "containers":[
                            {"name": deployment_name, 
                             "startupProbe": {"httpGet": {"path": "/health", "port": 8000},
                                              "initialDelaySeconds": 5,
                                              "periodSeconds": 5}
                            }
                        ]}}}})
                ]
            )
        
        if check_live:
            logging.info("will check live")

            p = sp.run([
                "kubectl",
                "-n", namespace, 
                "rollout",
                "status",
                "-w",
                "--timeout", "10m",
                "deployment",
                deployment_name,
            ], check = True)
            
            # TODO: redundant?
            for i in range(3):
                try:
                    p = sp.Popen([
                        "kubectl",
                        "exec",
                        #"-it",
                        f"deployments/{check_live_through}",
                        "-n",
                        namespace,
                        "--",
                        "bash", "-c",
                        f"curl {deployment_name}:8000"], stdout=sp.PIPE)
                    p.wait()
                    if p.stdout is not None:
                        service_output_json = p.stdout.read()
                        logger.info("got valid output: %s", service_output_json)
                        service_output = json.loads(service_output_json.decode())
                        logger.info("got valid output json: %s", service_output)
                        break
                except Exception as e:
                    logging.info("backend not yet deployed or problem getting response: %s", e)
                    time.sleep(10)                    
        else:
            service_output = {}
        
        return {
            "deployment_name": deployment_name,
            "namespace": namespace,
            "description": container['descr'],
            "image": container['image'],
            "author": container['author'],
            "last_change_time": container['last_change_time'],
            "workflow_dispatcher_signature": container['workflow_dispatcher_signature'],
            "workflow_nb_signature": container['workflow_nb_signature'],
            "service_output": service_output
        }


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('repository', metavar='repository', type=str)
    parser.add_argument('deployment_name', metavar='deployment_name', type=str)
    parser.add_argument('--namespace', metavar='namespace', type=str, default="oda-staging")
    parser.add_argument('--local', action="store_true", default=False)
    parser.add_argument('--build-engine', metavar="build_engine", default="docker")
    parser.add_argument('--nb2wversion', metavar="nb2wversion", default=version())
    
    args = parser.parse_args()

    setup_logging()
    
    deploy(args.repository, 
           args.deployment_name, 
           namespace=args.namespace, 
           local=args.local, 
           build_engine=args.build_engine, 
           nb2wversion=args.nb2wversion)


if __name__ == "__main__":
    main()
