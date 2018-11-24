from distutils.core import setup

setup(name='nb2workflow',
      version='1.0',
      description='convert notebook to workflow',
      author='Volodymyr Savchenko',
      author_email='contact@volodymyrsavchenko.com',
      license='GPLv3',
      packages=['nb2workflow'],
      zip_safe=False,

      entry_points={
          'console_scripts': [
            'nb2service = nb2workflow.service:main',
            'nb2worker = nb2workflow.container:main',
            ]
      },

      url = 'https://github.com/volodymyrss/nb2workflow',
      download_url = 'https://github.com/volodymyrss/nb2workflow/archive/1.0.tar.gz',
      keywords = ['jupyter', 'docker'],
      classifiers = [],
     )

