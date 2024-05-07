import json
import pytest
import subprocess as sp


@pytest.mark.deploy
def test_deploy():
    from nb2workflow.deploy import deploy
    deploy("https://renkulab.io/gitlab/vladimir.savchenko/oda-sdss", "legacysurvey")


def test_extract_resource_requirements(temp_dir, ontology_path):
    from nb2workflow.deploy import NBRepo
    test_repo = "https://github.com/okolo/s3test.git"
    with NBRepo(test_repo, ontology_path=ontology_path) as repo:
        resources = repo.pre_build_metadata['resources']
        expected_resources = {
            'crbeams3': {
                'resource': 'CRBeamS3',
                'required': True,
                'env_vars': {'CRBEAM_S3_CREDENTIALS2', 'CRBEAM_S3_CREDENTIALS'}
            }
        }
        assert expected_resources == resources


@pytest.mark.deploy
def test_deploy_secret(ontology_path):
    from nb2workflow.deploy import deploy, verify_resource_secret

    namespace = "oda-staging"
    secret_name = "crbeams3"
    credentials = dict(
        endpoint="play.min.io",
        access_key="Q3AM3UQ867SPQQA43P2F",
        secret_key="zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
    test_repo = "https://github.com/okolo/s3test.git"
    deploy_name = "s3test"
    docker_registry = "odahub"  # "okolo0"

    secret=json.dumps(credentials)
    with open('tmp_secret', 'w') as tmp_file:
        print(secret, file=tmp_file, end='')

    command = ["kubectl", "create", "secret", "generic", secret_name, "--from-file=credentials=tmp_secret",
             "-n", namespace]
    try:
        sp.check_call(command)
    except Exception:
        # cleanup
        sp.check_call(["kubectl", "delete", "secret", secret_name, "-n", namespace])
        sp.check_call(command)

    verify_resource_secret(secret_name, required=True, namespace=namespace)

    deploy(test_repo,
           deploy_name,
           namespace="oda-staging",
           local=False,
           run_tests=False,
           check_live=False,
           registry=docker_registry,
           ontology_path=ontology_path
           )

    def subprocess_cmd(command):
        process = sp.Popen(command, stdout=sp.PIPE, shell=True)
        proc_stdout = process.communicate()[0].strip()
        return proc_stdout.decode().split('\n')

    # Check if pod is running, find running pod name
    running_pods = subprocess_cmd(
        f"kubectl get pods -n {namespace} | grep {deploy_name}-backend" + " |  awk '$3==\"Running\" {print $1}'"
    )
    assert len(running_pods) > 0, 'No running pods found'

    # Check that environment variables are initialised inside the pod
    variables = subprocess_cmd(
        f"kubectl exec {running_pods[0]} -n {namespace} -- env  |  grep CRBEAM_S3_CREDENTIALS | grep {credentials['secret_key']}"
    )
    assert len(variables) == 2  #  s3test repo defines 2 environment variables: CRBEAM_S3_CREDENTIALS and CRBEAM_S3_CREDENTIALS2
