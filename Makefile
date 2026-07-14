.DEFAULT_GOAL := no_op

SRC = src/yellowdog_ray/*.py src/yellowdog_ray/raydog/*.py src/yellowdog_ray/utils/*.py examples/builder/*.py examples/autoscaler/*.py
STUBS = src/yellowdog_ray/*.pyi src/yellowdog_ray/raydog/*.pyi src/yellowdog_ray/utils/*.pyi
TESTS =
BUILD_DIST = build dist src/yellowdog_ray.egg-info
PYCACHE = __pycache__

VERSION_FILE := src/yellowdog_ray/__init__.py
VERSION := $(shell grep "__version__ =" $(VERSION_FILE) | sed -E 's/.*"([^"]+)".*/\1/')

# AMI build settings (see packer/README.md)
PYTHON_VERSION ?= 3.12.11
RAY_VERSION ?= 2.56.0
RAYDOG_VERSION ?=
AWS_REGION ?= eu-west-2
SSH_PUBLIC_KEY ?=
SUBNET_ID ?=
EXTRA_PIP_PACKAGES ?=

PACKER_VARS = -var "python_version=$(PYTHON_VERSION)" \
              -var "ray_version=$(RAY_VERSION)" \
              -var "raydog_version=$(RAYDOG_VERSION)" \
              -var "aws_region=$(AWS_REGION)" \
              -var "ssh_public_key=$(SSH_PUBLIC_KEY)" \
              -var "subnet_id=$(SUBNET_ID)" \
              -var "extra_pip_packages=$(EXTRA_PIP_PACKAGES)"

build: $(SRC) stubs
	python -m build

stubs: $(SRC)
	rm -f $(STUBS)
	stubgen -o src src/yellowdog_ray/raydog src/yellowdog_ray/utils --include-private

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

ami-validate:
	cd packer && packer init . && packer validate $(PACKER_VARS) .

ami: ami-validate
	cd packer && packer build $(PACKER_VARS) .

no_op:
	# Available targets are: build, clean, install, uninstall, format, update, docs,
	# docs-build-image, docs-publish-image, pypi-check-build, stubs,
	# pypi-test-upload, pypi-prod-upload, ami, ami-validate
