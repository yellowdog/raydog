#!/bin/bash

# For development only

PYFILES="raydog/*.py *.py utils/*.py ray-jobs/*.py"
isort --profile=black $PYFILES && black --preview $PYFILES
