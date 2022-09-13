import argparse
import json
import logging
import os
import pathlib
import re
import subprocess
import tempfile
import time
import yaml
from .logging_setup import setup_logging
from . import version

logger = logging.getLogger(__name__)

default_config = {
    "config_schema_version": "0.1.0",
    "notebook_path": "", # or "notebooks"
    "extra_data": [],
    "use_repo_base_image": False
}


#TODO: probably want an option to really use the dir
def determine_origin(repo):
    if os.path.isdir(repo):
        return subprocess.check_output(
            ["git", "remote", "get-url", "origin"],
            cwd=repo).decode().strip()
    else:
        return repo

def deploy(git_origin, deployment_base_name, namespace="oda-staging", local=False, run_tests=True, check_live=True):
    git_origin = determine_origin(git_origin)

    with tempfile.TemporaryDirectory() as tmpdir:        
        subprocess.check_call(# cli is more stable than python API
            ["git", "clone", git_origin, "nb-repo"],
            cwd=tmpdir)

        local_repo_path = pathlib.Path(tmpdir) / "nb-repo"

        descr = subprocess.check_output( # cli is more stable than python API
            ["git", "describe", "--always", "--tags"],
            cwd=local_repo_path ).decode().strip()
        
        author = subprocess.check_output( 
            ["git", "log", "-1", "--pretty=format:'%an <%ae>'"], # could use all authors too, but it's inside anyway
            cwd=local_repo_path ).decode().strip()
            
        last_change_time = subprocess.check_output( 
            ["git", "log", "-1", "--pretty=format:'%ai'"], # could use all authors too, but it's inside anyway
            cwd=local_repo_path ).decode().strip()
            

        config_fn = local_repo_path / "mmoda.yaml"

        config = default_config.copy()
        if os.path.exists(config_fn):
            extra_config = yaml.safe_load(open(config_fn))
            logger.info("extra config from %s: %s", config_fn, extra_config)
            config.update(extra_config)
        else:
            logger.info("no extra config in %s", config_fn)
        logger.info("complete config: %s", config)

        if not config['use_repo_base_image']: 
            notebook_fullpath_in_container = pathlib.Path('/repo') / (config['notebook_path'].strip("/"))

            logger.info("using notebook_fullpath_in_container: %s", notebook_fullpath_in_container)

            open(pathlib.Path(tmpdir) / "Dockerfile", "a").write(f"""
FROM python:3.9

ADD nb-repo/requirements.txt /requirements.txt
RUN pip install -r requirements.txt

""")

        # we could use completely new image too. but lets keep renku etc in it
        open(pathlib.Path(tmpdir) / "Dockerfile", "a").write(f"""
RUN pip install nb2workflow[cwl,service,rdf]=={version()}

ENV ODA_WORKFLOW_VERSION="{descr}"
ENV ODA_WORKFLOW_LAST_AUTHOR="{author}"
ENV ODA_WORKFLOW_LAST_CHANGED="{last_change_time}"
ENV ODA_WORKFLOW_NOTEBOOK_PATH="{notebook_fullpath_in_container}"

COPY nb-repo/ /repo/

RUN curl -o /usr/bin/jq -L https://github.com/stedolan/jq/releases/download/jq-1.5/jq-linux64; chmod +x /usr/bin/jq
RUN for nn in $ODA_WORKFLOW_NOTEBOOK_PATH/*.ipynb; do mv $nn $nn-tmp;  jq '.metadata.kernelspec.name |= "python3"' $nn-tmp > $nn ; rm $nn-tmp ; done

ENTRYPOINT nb2service --debug $ODA_WORKFLOW_NOTEBOOK_PATH --host 0.0.0.0 --port 8000 | cut -c1-500
""")

        image = f"odahub/nb-{pathlib.Path(git_origin).name}:{descr}-nb2w{version()}" # {time.strftime(r'%y%m%d%H%M%S')}"

        subprocess.check_call( # cli is more stable than python API
            ["docker", "build", ".", "-t", image],
            cwd=tmpdir)        

        if run_tests: 
            # TODO: run tests too
            out = subprocess.check_output(
                    ["docker", "run", '--rm', '--entrypoint', 'bash', image, '-c', 
                     ('pip install nb2workflow[rdf,mmoda,service] --upgrade;'
                      'for a in $(ls $ODA_WORKFLOW_NOTEBOOK_PATH/*ipynb | grep -v test_); do'
                      '  nbinspect --machine-readable $a;'
                      '  nbrun --machine-readable $a;'
                      'done')
                     ],
                    cwd=tmpdir)

            workflow_dispatcher_signature = json.loads(re.search(rb"^WORKFLOW-DISPATCHER-SIGNATURE: (.*?)$", out, re.M).group(1).decode())
            workflow_nb_signature = json.loads(re.search(rb"^WORKFLOW-NB-SIGNATURE: (.*?)$", out, re.M).group(1).decode())
        else:
            workflow_dispatcher_signature = None
            workflow_nb_signature = None

        if local:
            subprocess.check_call( # cli is more stable than python API
                ["docker", "run", '-p', '8000:8000', image],
                cwd=tmpdir)
        else:
            subprocess.check_call( # cli is more stable than python API
                ["docker", "push", image],
                cwd=tmpdir)


            deployment_name = deployment_base_name + "-backend"
            try:
                subprocess.check_call(
                    ["kubectl", "patch", "deployment", deployment_name, "-n", namespace,
                    "--type", "merge",
                    "-p", 
                    json.dumps(
                        {"spec":{"template":{"spec":{
                            "containers":[
                                {"name": deployment_name, "image": image}
                            ]}}}})
                    ]
                )
            except Exception as e:
                subprocess.check_call(
                    ["kubectl", "create", "deployment", deployment_name, "-n", namespace, "--image=" + image]
                )
                subprocess.check_call(
                    ["kubectl", "expose", "deployment", deployment_name, "--name", deployment_name, 
                    "--port", "8000", "-n", namespace]
                )

            # try:
            #     cmd = ["kubectl", "create", "ingress", "annotated",  
            #          f'--rule="{deployment_name}-workflow-backend.obsuks1.unige.ch={deployment_name}/*:8000"',
            #          "--annotation", "traefik.ingress.kubernetes.io/router.entrypoints=websecure",
            #          "--annotation", "traefik.ingress.kubernetes.io/router.tls=true",
            #          ]

            #     logger.info(" ".join(cmd))
            #     subprocess.check_call(
            #         cmd
            #     )
            # except Exception:
            #     raise

            if check_live:
                print("\033[31mwill check live\033[0m")
                while True:
                    try:
                        p = subprocess.Popen([
                            "kubectl",
                            "exec",
                            "-it",
                            "deployments/oda-dispatcher",
                            "-n",
                            "oda-staging",
                            "--",
                            "bash", "-c",
                            f"curl {deployment_name}:8000"], stdout=subprocess.PIPE)
                        print("p", p)
                        p.wait()
                        if p.stdout is not None:
                            service_output_json = p.stdout.read()
                    except Exception as e:
                        print("problem getting response from the service:", service_output_json)
                        time.sleep(3)
                    else:
                        print("got valid output:", service_output_json)
                        service_output = json.loads(service_output_json.decode())
                        print("got valid output json:", service_output)
                        break
            else:
                service_output = {}
            
            return {
                "deployment_name": deployment_name,
                "namespace": namespace,
                "description": descr,
                "image": image,
                "author": author,
                "last_change_time": last_change_time,
                "workflow_dispatcher_signature": workflow_dispatcher_signature,
                "workflow_nb_signature": workflow_nb_signature,
                "service_output": service_output
            }


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('repository', metavar='repository', type=str)
    parser.add_argument('deployment_name', metavar='deployment_name', type=str)
    parser.add_argument('--namespace', metavar='namespace', type=str, default="oda-staging")
    parser.add_argument('--local', action="store_true", default=False)
    
    args = parser.parse_args()

    logging.basicConfig
    setup_logging()
    
    deploy(args.repository, args.deployment_name, namespace=args.namespace, local=args.local)


if __name__ == "__main__":
    main()
