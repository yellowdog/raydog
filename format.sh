#!/bin/bash

# For development only

PYFILES="*.py raydog/*.py utils/*.py ray-jobs/*.py"
isort --profile=black $PYFILES && black --preview $PYFILES
