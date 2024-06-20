[![PyPI version](https://badge.fury.io/py/nb2workflow.svg)](https://badge.fury.io/py/nb2workflow)
[![codebeat badge](https://codebeat.co/badges/79285797-5d5b-4770-87dd-35e5dad68729)](https://codebeat.co/projects/github-com-volodymyrss-nb2workflow-master)
[![Python package](https://github.com/volodymyrss/nb2workflow/actions/workflows/python-package.yml/badge.svg)](https://github.com/volodymyrss/nb2workflow/actions/workflows/python-package.yml)
[![codecov](https://codecov.io/gh/volodymyrss/nb2workflow/branch/master/graph/badge.svg)](https://codecov.io/gh/volodymyrss/nb2workflow)

This repository helps to use notebook as a workflow.

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

Building and deploying RESTful (Swagger) service:

```bash
$ nb2deploy https://renkulab.io/gitlab/vladimir.savchenko/oda-sdss legacysurvey
```

Alternatively, using `kaniko`:

```bash
$ nb2deploy https://renkulab.io/gitlab/astronomy/mmoda/fermi fermi-an-test --build-engine kaniko
```

## Motivation

See development guide for details https://odahub.io/docs/guide-development/

## Why run notebooks?

### Why convert notebook as a service?

