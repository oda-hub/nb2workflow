import pytest
import subprocess as sp

@pytest.mark.deploy
def test_deploy():
    from nb2workflow.deploy import deploy

    deploy("https://renkulab.io/gitlab/vladimir.savchenko/oda-sdss", "legacysurvey")

@pytest.mark.deploy
def test_deploy_secret():
    import json
    from nb2workflow.deploy import deploy, verify_s3_secret
    namespace = "oda-staging"
    secret_name = "crbeams3"
    credentials = dict(
        endpoint="play.min.io",
        access_key="Q3AM3UQ867SPQQA43P2F",
        secret_key="zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
    test_repo = "https://github.com/okolo/s3test.git"
    deploy_name = "s3test"


    secret=json.dumps(credentials)
    with open('tmp_secret', 'w') as tmp_file:
        print(secret, file=tmp_file, end='')

    try:
        sp.check_call(
            ["kubectl", "create", "secret", "generic", secret_name, "--from-file=credentials=tmp_secret",
             "-n", namespace]
        )

        verify_s3_secret(secret_name, namespace=namespace)

        result = deploy(test_repo,
               deploy_name,
               namespace="oda-staging",
               local=False,
               run_tests=False,  # TODO: enable output check
               check_live=False,
               registry="odahub")
        service_output = result['service_output']  # TODO: enable output check
    finally:
        # cleanup
        sp.check_call(
            ["kubectl", "delete", "secret", secret_name, "-n", namespace]
        )

