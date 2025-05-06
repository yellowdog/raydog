#!/bin/bash

# For development only

PYFILES="raydog/*.py *.py utils/*.py"
isort --profile=black $PYFILES && black --preview $PYFILES
