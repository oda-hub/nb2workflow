from __future__ import annotations

import argparse
from functools import cached_property, lru_cache
import json
import re
import logging
import os
import pathlib
import re
import shutil
import subprocess as sp
import tempfile
import time
import yaml
from .logging_setup import setup_logging
from . import version
from textwrap import dedent
from kubernetes import client, config
import glob
import rdflib
from oda_api.ontology_helper import Ontology
from jinja2 import Environment, PackageLoader
from nb2workflow.nbadapter import NotebookAdapter

logger = logging.getLogger(__name__)

default_config = {
    "config_schema_version": "0.1.0",
    "notebook_path": "", # or "notebooks"
    "extra_data": [],
    "use_repo_base_image": False,
    "filename_pattern": '.*', 
}

default_ontology_path = "https://odahub.io/ontology/ontology.ttl"

default_python_version = '3.10'

jenv = Environment(loader=PackageLoader('nb2workflow')) 

#TODO: probably want an option to really use the dir
def determine_origin(repo):
    if os.path.isdir(repo):
        return sp.check_output(
            ["git", "remote", "get-url", "origin"],
            cwd=repo).decode().strip()
    else:
        return repo

def check_job_status(job_name, namespace="default"):
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
        # bot does this in pod generally, but still allows to operate externally
    batch_v1 = client.BatchV1Api()
    response = batch_v1.read_namespaced_job_status(job_name, namespace)
    return response.status.conditions


class ContainerBuildException(Exception): 
    def __init__(self, message = '', dockerfile=None, buildlog=None):
        super().__init__(message)
        self.buildlog = buildlog
        self.dockerfile = dockerfile

class DeploymentException(Exception): ...


class NBRepo:
    def __init__(self, 
                 repo_path: str,
                 registry: str = "odahub",
                 ontology_path: str = default_ontology_path):
        self.git_origin = determine_origin(repo_path)
        self.ontology = Ontology(ontology_path)
        self.registry = registry
    
    @property
    def context_dir(self) -> pathlib.Path:
        if not hasattr(self, '_tempdir'):
            self._tempdir = self._make_tmpdir()
        return pathlib.Path(self._tempdir)
    
    @property
    def local_repo_path(self) -> pathlib.Path:
        if not hasattr(self, '_repopath'):
            self._clone_repo_to_context()
            self._repopath = self.context_dir / "nb-repo"
        return self._repopath

    def _clone_repo_to_context(self):
        sp.check_call(["git", "clone", self.git_origin, "nb-repo"],
                        cwd=self.context_dir)
    
    def _make_tmpdir(self) -> str:
        tmpdir = tempfile.mkdtemp()
        logger.info('Creating tempdir: %s', tmpdir)
        return tmpdir
        
    def cleanup(self):
        if hasattr(self, '_tempdir'):
            shutil.rmtree(self._tempdir)
            del(self._tempdir)

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.cleanup()

    @cached_property
    def pre_build_metadata(self) -> dict[str,str|dict]:
        meta = {}
        meta['descr'] = sp.check_output(
                            ["git", "describe", "--always", "--tags"],
                            cwd=self.local_repo_path ).decode().strip()
        
        meta['author'] = sp.check_output( 
                            ["git", "log", "-1", "--pretty=format:'%an <%ae>'"],
                            cwd=self.local_repo_path ).decode().strip()
            
        meta['last_change_time'] = sp.check_output( 
                                    ["git", "log", "-1", "--pretty=format:'%ai'"],
                                    cwd=self.local_repo_path ).decode().strip()

        meta['resources'] = self.resource_requirements

        return {"origin": self.git_origin,
                "descr": meta['descr'],
                "author": meta['author'],
                "last_change_time": meta['last_change_time'],
                "resources": meta["resources"]}
    
    @lru_cache
    def generate_dockerfile(self,
                            source_from: str, 
                            nb2wversion: str) -> str:
        if source_from not in ['localdir', 'git']:
            raise NotImplementedError('Unsupported source code location %s', source_from)
        
        tmpl = jenv.get_template('Dockerfile.jinja')
            
        config_fn = self.local_repo_path / "mmoda.yaml"

        config = default_config.copy()
        
        if os.path.exists(config_fn):
            with open(config_fn, 'r') as fd:
                extra_config = yaml.safe_load(fd)
            logger.info("extra config from %s: %s", config_fn, extra_config)
            config.update(extra_config)
        else:
            logger.info("no extra config in %s", config_fn)   
        logger.info("complete config: %s", config)


        if config['use_repo_base_image']: 
            with open(self.context_dir / "Dockerfile", "r") as fd:
                dockerfile_base = fd.read()
        else:
            dockerfile_base = None
        
        notebook_fullpath_in_container = pathlib.Path('/repo') / (config['notebook_path'].strip("/"))
        logger.info("using notebook_fullpath_in_container: %s", notebook_fullpath_in_container)

        # will be used only if not use_repo_base_image but need to define to pass into template
        has_conda_env = False
        inject_python_version_str = ''
      
        if not config['use_repo_base_image']:
            inject_python_version_str = f"sed -i '/dependencies/a \ \ - python={default_python_version}' /repo/environment.yml"
            if os.path.exists( self.local_repo_path / 'environment.yml' ):
                with open(self.local_repo_path / 'environment.yml') as fd:
                    parsed_env = yaml.safe_load(fd)
                    if 'dependencies' in parsed_env:
                        has_conda_env = True
                        match_spec = re.compile(r'^python[~=<> ]')
                        for dep in parsed_env['dependencies']:
                            if isinstance(dep, str) and match_spec.match(dep):
                                inject_python_version_str = f'echo "Using {dep}"'
                                break

        if nb2wversion.startswith('git+'):
            nb2w_version_spec = f"git+https://github.com/oda-hub/nb2workflow@{nb2wversion[4:]}#egg=nb2workflow[service]"
        else:
            nb2w_version_spec = f"nb2workflow[service]=={nb2wversion}"
        
        dockerfile_content = tmpl.render(
            dockerfile_base = dockerfile_base,
            source_from = source_from,
            git_origin = self.git_origin,
            has_conda_env = has_conda_env,
            inject_python_version_str = inject_python_version_str,
            default_python_version = default_python_version,
            nb2w_version_spec = nb2w_version_spec,
            metadata = self.pre_build_metadata,
            nbpath = notebook_fullpath_in_container,
            filename_pattern = config['filename_pattern']
        )
        
        with open(self.context_dir / "Dockerfile", "w") as fd:
            fd.write(dockerfile_content)
        
        return dockerfile_content

    @cached_property
    def resource_requirements(self) -> dict[str, dict]:
        resources = {}

        search_pattern = os.path.join(self.context_dir,'**/*.ipynb')
        for nb_file in glob.glob(search_pattern, recursive=True):
            nba = NotebookAdapter(nb_file)
            g = nba._graph
            for r in self.ontology.get_requested_resources(g):
                resource_name = r['resource'].lower()
                if resource_name in resources:
                    resource_settings = resources[resource_name]
                    resource_settings['required'] = resource_settings['required'] or r['required']
                    resource_settings['env_vars'] = r['env_vars'].union(resource_settings['env_vars'])
                else:
                    resources[resource_name] = r

        return resources

    def build_with_docker(self,
                          no_push: bool = True, 
                          build_timestamp: bool = False,
                          source_from: str = 'localdir',
                          nb2wversion: str = version(print_it=False)) -> dict:

        meta = self.pre_build_metadata
        dockerfile_content = self.generate_dockerfile(source_from=source_from, nb2wversion=nb2wversion)

        ts = '-' + time.strftime(r'%y%m%d%H%M%S') if build_timestamp else ''
        if no_push:
            image = (f"nb-{pathlib.Path(self.git_origin).name}:{meta['descr']}"
                     f"-nb2w{nb2wversion.replace('git+', '')}{ts}")
        else:
            image = (f"{self.registry}/nb-{pathlib.Path(self.git_origin).name}:{meta['descr']}"
                     f"-nb2w{nb2wversion.replace('git+', '')}{ts}")
            
        sp.check_call(
            ["docker", "build", ".", "-t", image],
            cwd=self.context_dir)
            
        if not no_push: 
            sp.check_call(
                ["docker", "push", image])
        
        self.built_container_metadata = {
                "origin": meta["origin"],
                "descr": meta['descr'],
                "image": image,
                "author": meta['author'],
                "last_change_time": meta['last_change_time'],
                "dockerfile_content": dockerfile_content,
                "resources": meta["resources"]}
        return self.built_container_metadata
    
    def run_tests(self):
        logger.warning('Tests are not currently implemented')

    def build_with_kaniko(self,
                          no_push: bool = False,
                          build_timestamp: bool = False,
                          namespace: str = "oda-staging",
                          cleanup: bool = True,
                          nb2wversion=version(print_it=False),
                          kaniko_pod_antiaffinity=True,
                          dispatcher_app_label='dispatcher',
                          frontend_app_label='frontend') -> dict:
       
        #secret should be created beforehand https://github.com/GoogleContainerTools/kaniko#pushing-to-docker-hub
        
        meta = self.pre_build_metadata
        dockerfile_content = self.generate_dockerfile(source_from='git', nb2wversion=nb2wversion)

        suffix = self.context_dir.name.lower().replace('_', '-').rstrip('-')

        if no_push:
            image = None
        else:  
            ts = '-' + time.strftime(r'%y%m%d%H%M%S') if build_timestamp else ''
            image = (f"{self.registry}/nb-{pathlib.Path(self.git_origin).name}:{meta['descr']}"
                     f"-nb2w{nb2wversion.replace('git+', '')}{ts}")

        os.makedirs(self.context_dir/'build', exist_ok=True)

        tmpl = jenv.get_template('buildjob.yaml.jinja')
        with open(self.context_dir/'build'/'buildjob.yaml', 'w') as fd:
            fd.write(tmpl.render(
                suffix = suffix,
                namespace = namespace,
                no_push = no_push,
                image = image,
                kaniko_pod_antiaffinity = kaniko_pod_antiaffinity,
                dispatcher_app_label = dispatcher_app_label,
                frontend_app_label = frontend_app_label
            ))

        tmpl = jenv.get_template('dockerfile_cm.yaml.jinja')
        with open(self.context_dir/'build'/'dockerfile_cm.yaml', 'w') as fd:
            fd.write(tmpl.render(
                name = f"nb2w-dockerfile-{suffix}",
                namespace = namespace,
                content = dockerfile_content
            ))

        try:                   
            sp.check_call([
                "kubectl",
                "apply",
                "-f",
                "build"
            ], cwd=self.context_dir)
            
            while True:
                time.sleep(10)
                job_status = check_job_status(f"kaniko-build-{suffix}", namespace)
                if job_status is not None:
                    if 'Complete' in [x.type for x in job_status]:
                        break
                    if 'Failed' in [x.type for x in job_status]:
                        try:
                            buildlog = sp.check_output([
                                'kubectl',
                                'logs',
                                f"job/kaniko-build-{suffix}"
                                ])
                        except sp.CalledProcessError:
                            buildlog = 'Not available'

                        raise ContainerBuildException('', 
                                                      dockerfile=dockerfile_content, 
                                                      buildlog=buildlog)
        finally:
            if cleanup:
                sp.check_call([
                    "kubectl",
                    "delete",
                    "-f",
                    "build"
                ], cwd = self.context_dir)
        
        self.built_container_metadata = {
                "origin": meta["origin"],
                "descr": meta['descr'],
                "image": image,
                "author": meta['author'],
                "last_change_time": meta['last_change_time'],
                "dockerfile_content": dockerfile_content,
                "resources": meta["resources"]}
        return self.built_container_metadata

    def deploy_with_docker(self, 
                           container_base_name: str,
                           container_override: dict|None = None
                           ):
        if container_override is None:
            if not hasattr(self, 'built_container_metadata'):
                self.build_with_docker()
            use_container_meta = self.built_container_metadata
        else:
            use_container_meta = container_override

        container_name = container_base_name + "-backend"
        
        env_params = []
        for name, resource in use_container_meta['resources'].items():
            for env in resource['env_vars']:
                env_val = os.getenv(env)
                if env_val:
                    env_params += ["-e", env]
                elif resource['required']:
                    raise RuntimeError(f'Required environment variable {env} is missing')

        try:
            sp.check_call(
                ["docker", "run", '-p', '8000:8000'] + 
                ["--name", container_name] +
                env_params + 
                [use_container_meta['image']])
        except sp.CalledProcessError as e:
            logger.error('Deployment failed: %s', e, exc_info=True)
            raise DeploymentException(str(e))

        return {
            "container_name": container_name,
            "description": use_container_meta['descr'],
            "image": use_container_meta['image'],
            "author": use_container_meta['author'],
            "last_change_time": use_container_meta['last_change_time'],
            "workflow_dispatcher_signature": None, # TODO: not applicable?
            "workflow_nb_signature": None, # TODO: do exec inplace?
        }

    def deploy_k8s(self, 
                   deployment_base_name: str, 
                   namespace: str = "oda-staging",
                   with_volume: bool = True,
                   storageclass: str = 'nfs',
                   rw_many: bool = True,
                   volume_size: int = 20,
                   container_override: dict|None = None,
                   ):
        if container_override is None:
            if not hasattr(self, 'built_container_metadata'):
                self.build_with_kaniko(namespace=namespace)
            use_container_meta = self.built_container_metadata
        else:
            use_container_meta = container_override

        deployment_name = deployment_base_name + '-backend'

        secretenv = []
        for name, resource in use_container_meta['resources'].items():
            verify_resource_secret(name, resource['required'], namespace=namespace)
            for env in resource['env_vars']:
                secretenv.append((env, name))

        os.makedirs(self.context_dir/'deploy', exist_ok=True)

        tmpl = jenv.get_template('deployment.yaml.jinja')
        with open(self.context_dir/'deploy'/'deployment.yaml', 'w') as fd:
            fd.write(tmpl.render(
                deployment_name = deployment_name,
                namespace = namespace,
                with_volume = with_volume,
                rw_many = rw_many,
                secretenv = secretenv,
                image = use_container_meta['image']
            ))

        tmpl = jenv.get_template('pvc.yaml.jinja')
        with open(self.context_dir/'deploy'/'pvc.yaml', 'w') as fd:
            fd.write(tmpl.render(
                deployment_name = deployment_name,
                namespace = namespace,
                with_volume = with_volume,
                rw_many = rw_many,
                volume_size = volume_size,
                storageclass = storageclass
                ))
        
        tmpl = jenv.get_template('service.yaml.jinja')
        with open(self.context_dir/'deploy'/'service.yaml', 'w') as fd:
            fd.write(tmpl.render(
                deployment_name = deployment_name,
                namespace = namespace
                ))

        try:
            sp.check_call(['kubectl', 'apply', '-f', str(self.context_dir/'deploy')])
            p = sp.run([
                "kubectl",
                "-n", namespace, 
                "rollout",
                "status",
                "-w",
                "--timeout", "10m",
                "deployment",
                deployment_name,
            ], check=True)
        except sp.CalledProcessError as e:
            logger.error('Deployment failed: %s', e, exc_info=True)
            sp.run(['kubectl', 'delete', '-f', str(self.context_dir/'deploy')])
            raise DeploymentException(str(e))
       
        return {
            "deployment_name": deployment_name,
            "namespace": namespace,
            "description": use_container_meta['descr'],
            "image": use_container_meta['image'],
            "author": use_container_meta['author'],
            "last_change_time": use_container_meta['last_change_time'],
            "workflow_dispatcher_signature": None,
            "workflow_nb_signature": None,
        }

def build_container(git_origin, 
                    local=False, 
                    run_tests=True, 
                    registry="odahub", 
                    build_timestamp=False,
                    engine="docker",
                    cleanup=False,
                    nb2wversion=version(print_it=False),
                    ontology_path=default_ontology_path,
                    **kwargs):
    logger.warning('Function build_container is DEPRECATED. Please use NBRepo class methods.')
    with NBRepo(git_origin, 
                registry=registry,
                ontology_path=ontology_path) as repo:
        if run_tests: repo.run_tests()
        if engine == "docker":
            return repo.build_with_docker(no_push=local,
                                          build_timestamp=build_timestamp,
                                          nb2wversion=nb2wversion)
        elif engine == 'kaniko':
            return repo.build_with_kaniko(no_push=local,
                                          build_timestamp=build_timestamp,
                                          namespace=kwargs['namespace'],
                                          nb2wversion=nb2wversion)
        else:
            raise NotImplementedError('Unknown container build engine: %s', engine)

def get_k8s_secrets(namespace="oda-staging"):
    json_data = sp.check_output(["kubectl", "get", "secrets", "-n", namespace, "-o", "json"])
    items = json.loads(json_data)['items']
    for secret in items:
        yield secret['metadata']['name'], secret['data']

def verify_resource_secret(name, required, namespace="oda-staging"):
    # credentials
    for secret_name, secret in get_k8s_secrets(namespace=namespace):
        if secret_name == name:
            if 'credentials' not in secret:
                raise NameError(f"No credentials defined for secret {name}")
            return True
    message = f"No secrets defined for {name}"
    if required:
        raise RuntimeError(message)
    else:
        logger.warning(message)

def deploy_k8s(container_info, 
           deployment_base_name, 
           namespace="oda-staging", 
           **kwargs):
    logger.warning('Function build_container is DEPRECATED. Please use NBRepo class methods.')
    with NBRepo(container_info['origin']) as repo:
        return repo.deploy_k8s(
            deployment_base_name=deployment_base_name,
            namespace=namespace,
            container_override=container_info)

def deploy(git_origin, 
           deployment_base_name, 
           namespace="oda-staging", 
           local=False, 
           run_tests=True, 
           check_live=True, 
           registry="odahub", 
           check_live_through="oda-dispatcher",
           build_engine='docker',
           build_timestamp=False,
           cleanup=False,
           nb2wversion=version(),
           ontology_path=default_ontology_path):
    with NBRepo(git_origin,
                registry=registry,
                ontology_path=ontology_path) as repo:
        if build_engine == 'docker':
            repo.build_with_docker(no_push = local,
                                   nb2wversion= nb2wversion,
                                   build_timestamp=build_timestamp)
            res = repo.deploy_with_docker(deployment_base_name)

        elif build_engine == 'kaniko':
            repo.build_with_kaniko(namespace=namespace,
                                   nb2wversion=nb2wversion,
                                   build_timestamp=build_timestamp,
                                   cleanup=cleanup)
            res = repo.deploy_k8s(deployment_base_name=deployment_base_name,
                            namespace=namespace)

        else:
            raise NotImplementedError('Unknown build_engine')
        
    return res


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('repository', metavar='repository', type=str)
    parser.add_argument('deployment_name', metavar='deployment_name', type=str)
    parser.add_argument('--namespace', metavar='namespace', type=str, default="oda-staging")
    parser.add_argument('--local', action="store_true", default=False)
    parser.add_argument('--build-engine', metavar="build_engine", default="docker")
    parser.add_argument('--registry', metavar="build_engine", default="odahub")
    parser.add_argument('--nb2wversion', metavar="nb2wversion", default=version(print_it=False))
    parser.add_argument('--ontology-path', metavar="ontology_path", default=default_ontology_path)
    
    args = parser.parse_args()

    setup_logging()
    
    deploy(args.repository,
           args.deployment_name,
           namespace = args.namespace,
           registry = args.registry,
           ontology_path = args.ontology_path,
           build_engine = args.build_engine,
           local = args.local,
           nb2wversion=args.nb2wversion
           )

if __name__ == "__main__":
    main()
