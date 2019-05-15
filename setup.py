import sys

from setuptools import find_packages, setup

# Always reference code-origin
# https://github.com/django/django/blob/master/setup.py#L7

CURRENT_PYTHON = sys.version_info[:2]
REQUIRED_PYTHON = (3, 6)

if CURRENT_PYTHON < REQUIRED_PYTHON:
    sys.stderr.write("""
==========================
Unsupported Python version
==========================
This version of Bert requires Python {}.{}, but you're trying to
install it on Python {}.{}.
This may be because you are using a version of pip that doesn't
understand the python_requires classifier. Make sure you
have pip >= 9.0 and setuptools >= 24.2, then try again:
    $ python -m pip install --upgrade pip setuptools
    $ python -m pip install bert
This will install the latest version of Bert which works on your
version of Python
""".format(*(REQUIRED_PYTHON + CURRENT_PYTHON)))
    sys.exit(1)


EXCLUDE_FROM_PACKAGES = ['bert.bin']
version = '0.1.0'
description = 'A microframework for simple ETL solutions'

setup(
  name='Bert',
  version=version,
  python_requires='>={}.{}'.format(*REQUIRED_PYTHON),
  url='https://bert.jbcurtin.io/',
  author="Joseph Curtin <42@jbcurtin.io",
  author_email='42@jbcurtin.io',
  description=description,
  long_description=read('README.md'),
  license='MIT',
  packages=find_packages(exclude=EXCLUDE_FROM_PACKAGES),
  include_package_data=True,
  scripts=[
    'bert/bin/generate-bert-module',
    'bert/bin/bert-tutorial'
  ],
  classifiers=[],
  project_urls={}
)

