from setuptools import setup
import sys

setup_requires = ['setuptools >= 30.3.0', 'setuptools-git-version']
if {'pytest', 'test', 'ptr'}.intersection(sys.argv):
    setup_requires.append('pytest-runner')

setup(name='nb2workflow',
      version='1.0.1',
      version_format = '{tag}.dev{commitcount}+{gitsha}',
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
            'nb2cwl=nb2workflow.cwl:main',
            'nb2rdf=nb2workflow.ontology:main',
            'nbrun=nb2workflow.nbadapter:main',
            ]
      },

      install_requires=[
        'flask',
        'pytest-flask',
        'papermill',
        'ipykernel',
        'nbconvert',
        'docker',
        'checksumdir',
        'Flask-Caching',
        'flask-cors',
        'flasgger',
        'owlready2',
        'rdflib',
        'cwlgen',
      ],

      url = 'https://github.com/volodymyrss/nb2workflow',
      download_url = 'https://github.com/volodymyrss/nb2workflow/archive/1.0.1.tar.gz',
      keywords = ['jupyter', 'docker'],
      classifiers = [],
      setup_requires=setup_requires)



