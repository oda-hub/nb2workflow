import pytest

@pytest.mark.deploy
def test_deploy():
    from nb2workflow.deploy import deploy

    deploy("https://renkulab.io/gitlab/vladimir.savchenko/oda-sdss", "legacysurvey")