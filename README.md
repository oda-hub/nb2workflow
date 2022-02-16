[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c93b37d1f9874bbc8d4ec83cbf065313)](https://app.codacy.com/app/vladimir.savchenko/nb2workflow?utm_source=github.com&utm_medium=referral&utm_content=volodymyrss/nb2workflow&utm_campaign=Badge_Grade_Dashboard)
[![codebeat badge](https://codebeat.co/badges/79285797-5d5b-4770-87dd-35e5dad68729)](https://codebeat.co/projects/github-com-volodymyrss-nb2workflow-master)
[![Python package](https://github.com/volodymyrss/nb2workflow/actions/workflows/python-package.yml/badge.svg)](https://github.com/volodymyrss/nb2workflow/actions/workflows/python-package.yml)
[![codecov](https://codecov.io/gh/volodymyrss/nb2workflow/branch/master/graph/badge.svg)](https://codecov.io/gh/volodymyrss/nb2workflow)

## Starts service without container:
```bash
nb2service tests/testrepo/workflow-notebook.ipynb
```

## Builds service container and starts it:
```bash
nb2worker tests/testrepo/
```

## Builds one-shot container and cwl:

```bash
nb2worker tests/testrepo/ --build --job
```

## Generates cwl:
```bash
nb2cwl tests/testrepo/
```

## Deploys a service

```bash
nb2deploy https://renkulab.io/gitlab/vladimir.savchenko/oda-sdss legacysurvey
```
