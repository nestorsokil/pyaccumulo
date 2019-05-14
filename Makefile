.PHONY : clean build install test register check compile-proxy

clean:
	python setup.py clean --all
	rm -rf build dist src/*.egg-info/

build: clean
	python setup.py build

install: build
	pip install .

test: clean
	python setup.py test

register:
	python setup.py sdist bdist_egg upload -r pypi

check:
	pylint -i y --output-format=parseable src/`git remote -v | grep origin | head -1 | cut -d':' -f 2 | cut -d'.' -f 1`

compile-proxy:
	mkdir ./generated
	thrift --gen py -out ./generated ./resources/proxy.thrift
	rm -rf ./accumulo
	mkdir ./accumulo
	mv ./generated/accumulo/* ./accumulo
	rm -rf ./generated
	#2to3-3.7 -w -n ./pyaccumulo/proxy
