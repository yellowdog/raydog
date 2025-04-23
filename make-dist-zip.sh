#!/bin/bash

# Create resources for distribution

DIST="dist"
RAYDOG="raydog"

rm -rf "${DIST:?}/$RAYDOG"
mkdir -p "$DIST/$RAYDOG"

rm -rf raydog/__pycache__

for ITEM in README.md \
            usage-example.py \
            raydog \
            private_key
do
  cp -r $ITEM $DIST/$RAYDOG/.
done

chmod 0600 $DIST/$RAYDOG/private_key

cd $DIST || exit
ZIPFILE=raydog-"$(git rev-parse --short HEAD)".zip
echo "Creating $ZIPFILE ..."
zip -r $ZIPFILE $RAYDOG
echo "Done"
