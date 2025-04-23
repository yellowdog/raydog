#!/bin/bash

# For development only

PYFILES="raydog/*.py *.py"
isort --profile=black $PYFILES && black --preview $PYFILES
