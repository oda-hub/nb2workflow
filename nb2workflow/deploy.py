import json
import pathlib
import subprocess
import tempfile
import time

def deploy(git_origin, deployment_base_name):
    with tempfile.TemporaryDirectory() as tmpdir:        
        subprocess.check_call( # cli is more stable than python API
            ["git", "clone", git_origin, "nb-repo"],
            cwd=tmpdir)

        descr = subprocess.check_output( # cli is more stable than python API
            ["git", "describe", "--always", "--tags"],
            cwd=pathlib.Path(tmpdir) / "nb-repo" ).decode().strip()


        # 

        open(pathlib.Path(tmpdir) / "Dockerfile", "w").write("""
FROM python:3.8

ADD nb-repo/requirements.txt /requirements.txt

RUN pip install -r requirements.txt

RUN pip install nb2workflow[cwl,service,rdf]

ADD nb-repo/*.ipynb /repo/

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
                ["kubectl", "patch", "deployment", deployment_name, "-n", "oda-staging",
                "-p", 
                json.dumps(
                    {"spec":{"template":{"spec":{"containers":[{"image": image, "name": deployment_name}]}}}})
                ]
                # '{"spec":{"template":{"spec":{"containers":[{"image":"' + image + '","name":"' + deployment_name + '"}]}}}}']
            )
        except Exception as e:
            subprocess.check_call(
                ["kubectl", "create", "deployment", deployment_name, "-n", "oda-staging", "--image=" + image]
            )