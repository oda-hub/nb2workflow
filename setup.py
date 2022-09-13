from setuptools import setup
import sys

setup_requires = ['setuptools >= 30.3.0', 'setuptools-git-version']
if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    setup_requires.append('pytest-runner')

setup(name='nb2workflow',
      version = '1.3.34',
      description='convert notebook to workflow',
      author='Volodymyr Savchenko',
      author_email='contact@volodymyrsavchenko.com',
      license='GPLv3',
      packages=['nb2workflow'],
      zip_safe=False,

      entry_points={
          'console_scripts': [
            'nb2service=nb2workflow.service:main',
            'nb2worker=nb2workflow.container:main',
            'nb2workflow-version=nb2workflow:version',
            'nb2cwl=nb2workflow.cwl:main',
            'nb2rdf=nb2workflow.ontology:main',
            'nb2deploy=nb2workflow.deploy:main',
            'nbrun=nb2workflow.nbadapter:main',
            'nbinspect=nb2workflow.nbadapter:main_inspect',
            'nbreduce=nb2workflow.nbadapter:main_reduce',
            ]
      },
      
      extras_require={
        "service":[
            'flask',
            'pytest-flask',
            'flask-caching', 
            'flask-cors',
            'flasgger',
            'python-consul',
            'apscheduler',
        ],
        "rdf":[
            'rdflib',
            'owlready2==0.11',
            'oda-knowledge-base',
        ],
        "cwl":[
            "cwlgen",
        ],
        "docker":[
            'docker',
            'checksumdir',
        ],
        "domains":[
            'numpy',
            'astropy',
            'pandas',
            'matplotlib'
        ],
        "mmoda":[
            'dispatcher-plugin-nb2workflow'
        ]
      },

      tests_require=[
        'pytest',
        'pytest-xprocess',
        'matplotlib',
        'cwl-runner',
        'psutil'
      ],

      install_requires=[
        'papermill',
        'ipykernel',
        'nbconvert', 
        'psutil',
        'diskcache',
        'requests',
        'pyyaml',
        'scrapbook', 
        'werkzeug==2.0.3',
        'sentry_sdk'
      ],


      url = 'https://github.com/volodymyrss/nb2workflow',
      download_url = 'https://github.com/volodymyrss/nb2workflow/archive/1.0.1.tar.gz',
      keywords = ['jupyter', 'docker'],
      classifiers = [],
      setup_requires=setup_requires)



