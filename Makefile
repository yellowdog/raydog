.DEFAULT_GOAL := no_op

SRC = src/yellowdog_ray/*.py src/yellowdog_ray/raydog/*.py src/yellowdog_ray/utils/*.py examples/builder/*.py examples/autoscaler/*.py
STUBS = src/yellowdog_ray/*.pyi src/yellowdog_ray/raydog/*.pyi src/yellowdog_ray/utils/*.pyi
TESTS =
BUILD_DIST = build dist src/yellowdog_ray.egg-info
PYCACHE = __pycache__

VERSION_FILE := src/yellowdog_ray/__init__.py
VERSION := $(shell grep "__version__ =" $(VERSION_FILE) | sed -E 's/.*"([^"]+)".*/\1/')

build: $(SRC) stubs
	python -m build

stubs: $(SRC)
	rm -f $(STUBS)
	stubgen -o src src/yellowdog_ray/raydog src/yellowdog_ray/utils

clean:
	rm -rf $(BUILD_DIST) $(PYCACHE) $(STUBS)
	$(MAKE) -C docs clean

install: build
	pip install -U -e .

uninstall:
	pip uninstall -y yellowdog-ray

black: $(SRC) $(TESTS)
	black --preview $(SRC) $(TESTS)

isort: $(SRC)
	isort --profile black $(SRC) $(TESTS)

pyupgrade: $(SRC)
	pyupgrade --exit-zero-even-if-changed --py310-plus $(SRC) $(TESTS)

format: pyupgrade isort black

update:
	pip install -U -r requirements.txt -r requirements-dev.txt

.PHONY: docs

docs:
	$(MAKE) -C docs html

docs-build-image: docs
	cd docs && docker build . -t yellowdogco/raydog-docs:$(VERSION) --platform linux/amd64

docs-publish-image: docs-build-image
	docker push yellowdogco/raydog-docs:$(VERSION)

# See the ~/.pypirc file for the repository index
pypi-check-build: clean build
	twine check dist/*

pypi-test-upload: clean build
	python -m twine upload --repository yellowdog-testpypi dist/*

pypi-prod-upload: clean build
	python -m twine upload --repository yellowdog-ray dist/*

no_op:
	# Available targets are: build, clean, install, uninstall, format, update, docs,
	# docs-build-image, docs-publish-image, pypi-check-build, stubs
	# pypi-test-upload, pypi-prod-upload
