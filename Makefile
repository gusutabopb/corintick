test:
	flake8 --max-line-length=100 --ignore=F401,F841
	pytest --verbose --cov=corintick --cov-append --cov-report html tests/

clean:
	rm -rf build dist *.egg-info docs/_build/*
	rm -rf .cache htmlcov .coverage .pytest_cache
	rm -f .DS_Store README.html

build: clean test
	python setup.py sdist bdist_wheel

upload: build
	twine upload dist/*

upload-test: build
	twine upload --repository testpypi dist/*
