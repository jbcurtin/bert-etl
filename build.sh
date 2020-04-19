#!/usr/bin/env bash

set -e
# set -x
if [ "$1" == "docs" ]; then
    if [ ! -d "env-docs" ]; then
        virtualenv -p $(which python3) env-docs
        source env-docs/bin/activate
        pip install sphinx sphinx_rtd_theme -U
    else
        source env-docs/bin/activate
    fi
    mkdir -p /tmp/docs
    sphinx-build -b html docs/ /tmp/docs/

fi
if [ "$1" == "publish-prod" ]; then
    rm -rf dist
    rm -rf .eggs    
    rm -rf *.egg-info
    rm -rf build
    if [ ! -d "env-twine" ]; then
        virtualenv -p $(which python3) env-twine
        source env-twine/bin/activate
        pip install twine
    else
        source env-twine/bin/activate
    fi
    python setup.py sdist bdist_wheel --universal
    python -m twine upload --verbose dist/*
    echo 'Test with the following command, '
    echo '  $ pip install bert-etl -U'
fi

if [ "$1" == "publish-test" ]; then
    rm -rf dist
    rm -rf .eggs    
    rm -rf *.egg-info
    rm -rf build
    if [ ! -d "env-twine" ]; then
        virtualenv -p $(which python3) env-twine
        source env-twine/bin/activate
        pip install twine
    else
        source env-twine/bin/activate
    fi
    python setup.py sdist bdist_wheel --universal
    python -m twine upload --verbose --repository-url https://test.pypi.org/legacy/ dist/*
    echo 'Test with the following command, '
    echo '  $ pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple bert-etl -U'
fi

if [ "$1" == "setup-test" ]; then
    rm -rf env-test
    virtualenv -p $(which python3) env-test
    source env-test/bin/activate
    pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple bert-etl=$(cat VERSION)
fi
