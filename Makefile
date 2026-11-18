.DEFAULT_GOAL := help

# Everything installs into the active virtual environment with uv:
#   uv venv && source .venv/bin/activate && make install

VERSION_FILE := src/yellowdog_ray/__init__.py
VERSION := $(shell grep "^__version__ =" $(VERSION_FILE) | sed -E 's/.*"([^"]+)".*/\1/')

# AMI build settings (see packer/README.md)
PYTHON_VERSION ?= 3.12.11
RAY_VERSION ?= 2.60.0
RAYDOG_VERSION ?=
RAYDOG_PIP_SPEC ?=
AWS_REGION ?= eu-west-2
SSH_PUBLIC_KEY ?=
SUBNET_ID ?=
EXTRA_PIP_PACKAGES ?=

PACKER_VARS = -var "python_version=$(PYTHON_VERSION)" \
              -var "ray_version=$(RAY_VERSION)" \
              -var "raydog_version=$(RAYDOG_VERSION)" \
              -var "raydog_pip_spec=$(RAYDOG_PIP_SPEC)" \
              -var "aws_region=$(AWS_REGION)" \
              -var "ssh_public_key=$(SSH_PUBLIC_KEY)" \
              -var "subnet_id=$(SUBNET_ID)" \
              -var "extra_pip_packages=$(EXTRA_PIP_PACKAGES)"

.PHONY: help install lint format typecheck test check test-online test-instances test-sweep \
        build clean \
        docs docs-build-image docs-publish-image release release-test ami-validate ami update

help:
	@echo "Targets: install, update, lint, format, typecheck, test, check, test-online,"
	@echo "         test-instances, test-sweep,"
	@echo "         build, clean, docs, docs-build-image, docs-publish-image,"
	@echo "         release, release-test, ami-validate, ami"

install:
	uv pip install -e ".[dev,docs]"

# Upgrade every dependency, holding Ray at the version the examples install on
# nodes: Ray on this machine must match Ray on a cluster's nodes
update:
	uv pip install --upgrade -e ".[dev,docs]" "ray[default]==$(RAY_VERSION)"

lint:
	ruff check .
	ruff format --check .

format:
	ruff check --fix .
	ruff format .

typecheck:
	mypy

test:
	pytest

# Everything a release must pass
check: lint typecheck test docs

# Needs YellowDog credentials: see tests/README.md
test-online:
	pytest -m online

# The online tests that start instances (real `ray up` and the rest, about half an
# hour), then the sweep, whether they passed or not; the tests' result is the
# target's. Settings as for test-online; more pytest options in ARGS, e.g.
# make test-instances ARGS="-k container"
test-instances:
	pytest -m online_instances $(ARGS); status=$$?; python -m tests.sweep; exit $$status

# Remove anything online tests left behind, and check no instance is alive
test-sweep:
	python -m tests.sweep

build: clean
	python -m build
	twine check dist/*

clean:
	rm -rf build dist src/yellowdog_ray.egg-info docs/_build .pytest_cache .mypy_cache .ruff_cache .coverage
	find . -name __pycache__ -not -path "./.venv/*" -prune -exec rm -rf {} +

# -E: every page carries the whole navigation, so every page is rebuilt each time;
# an incremental build leaves old menus on pages whose own source has not changed
docs:
	sphinx-build -E -W --keep-going -b html docs docs/_build/html

docs-build-image: docs
	@test -n "$(VERSION)" || { echo "no __version__ found in $(VERSION_FILE)"; exit 1; }
	cd docs && docker build . -t yellowdogco/raydog-docs:$(VERSION) --platform linux/amd64

docs-publish-image: docs-build-image
	docker push yellowdogco/raydog-docs:$(VERSION)

# Releases are made locally: see RELEASING.md
release:
	python bin/release.py

release-test:
	python bin/release.py --test-pypi

ami-validate:
	cd packer && packer init . && packer validate $(PACKER_VARS) .

ami: ami-validate
	cd packer && packer build $(PACKER_VARS) .
