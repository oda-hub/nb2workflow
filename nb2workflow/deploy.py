import argparse
import json
import os
import pathlib
import subprocess
import tempfile
import time
import ruamel.yaml as yaml

default_config = {
    "notebook_dir": "notebooks",
    "use_repo_base_image": False
}

def deploy(git_origin, deployment_base_name, namespace="oda-staging"):
    with tempfile.TemporaryDirectory() as tmpdir:        
        subprocess.check_call( # cli is more stable than python API
            ["git", "clone", git_origin, "nb-repo"],
            cwd=tmpdir)

        local_repo_path = pathlib.Path(tmpdir) / "nb-repo"

        descr = subprocess.check_output( # cli is more stable than python API
            ["git", "describe", "--always", "--tags"],
            cwd=local_repo_path ).decode().strip()

        config_fn = local_repo_path / "mmoda.yaml"

        config = default_config.copy()
        if os.path.exists(config_fn):
            config.update(yaml.load(open(config_fn)))

        if not config['use_repo_base_image']: 
            open(pathlib.Path(tmpdir) / "Dockerfile", "a").write(f"""
FROM python:3.9

ADD nb-repo/requirements.txt /requirements.txt
RUN pip install -r requirements.txt

""")

        # we could use completely new image too. but lets keep renku etc in it
        open(pathlib.Path(tmpdir) / "Dockerfile", "a").write(f"""
RUN pip install nb2workflow[cwl,service,rdf]

ADD nb-repo/{config['notebook_dir']}/*.ipynb /repo/

RUN curl -o /usr/bin/jq -L https://github.com/stedolan/jq/releases/download/jq-1.5/jq-linux64; chmod +x /usr/bin/jq
RUN for nn in /repo/*.ipynb; do mv $nn $nn-tmp;  jq '.metadata.kernelspec.name |= "python3"' $nn-tmp > $nn ; rm $nn-tmp ; done

ENTRYPOINT nb2service /repo/ --host 0.0.0.0 --port 8000
""")

        image = f"odahub/nb-{pathlib.Path(git_origin).name}:{descr}-{time.strftime(r'%y%m%d%H%M%S')}"

        subprocess.check_call( # cli is more stable than python API
            ["docker", "build", ".", "-t", image],
            cwd=tmpdir)

        subprocess.check_call( # cli is more stable than python API
            ["docker", "push", image],
            cwd=tmpdir)


        deployment_name = deployment_base_name + "-backend"
        try:
            subprocess.check_call(
                ["kubectl", "patch", "deployment", deployment_name, "-n", namespace,
                "-p", 
                json.dumps(
                    {"spec":{"template":{"spec":{"containers":[{"image": image, "name": deployment_name}]}}}})
                ]
            )
        except Exception as e:
            subprocess.check_call(
                ["kubectl", "create", "deployment", deployment_name, "-n", "oda-staging", "--image=" + image]
            )


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('repository', metavar='repository', type=str)
    parser.add_argument('deployment_name', metavar='deployment_name', type=str)
    parser.add_argument('--namespace', metavar='namespace', type=str)
    
    args = parser.parse_args()
    
    deploy(args.repository, args.deployment_name)


if __name__ == "__main__":
    main()
