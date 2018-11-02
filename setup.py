from distutils.core import setup

setup(name='nb2workflow',
      version='1.0',
      description='convert notebook to workflow',
      author='Volodymyr Savchenko',
      author_email='vladimir.savchenko@gmail.com',
      license='MIT',
      packages=['nb2workflow'],
      zip_safe=False,

      entry_points={
          'console_scripts': [
            'nb2service = nb2workflow.workflow:main',
            ]
      },
     )
