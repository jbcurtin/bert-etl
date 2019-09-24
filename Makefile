

release:
	make clean
	python setup.py sdist bdist_wheel --universal
	python -m twine upload --verbose dist/*

clean :
	rm -rf dist
	rm -rf build
	rm -rf bert_etl.egg-info
    
install: clean
	pip uninstall bert-etl
	python setup.py install

