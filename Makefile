clean:
	rm -rf dist/

build:
	hatch build

publish:
	twine upload dist/*
