clean :
	rm -rf dist
	rm -rf build
	rm -rf bert_etl.egg-info
    
install: clean
	pip uninstall bert-etl
	python setup.py install

release: clean
	pip install -U twine
	pip install -U setuptools
	pip install -U pip
	python setup.py sdist
	python -m twine upload --verbose dist/*

build-docs:
	pip install sphinx sphinx_rtd_theme pip setuptools -U
	mkdir -p /tmp/docs
	rm -rf /tmp/docs/*
	sphinx-build -b html docs/ /tmp/docs
