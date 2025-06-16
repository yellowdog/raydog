#!/bin/bash

# Create resources for distribution

DIST="dist"
RAYDOG="raydog"
COMMIT=$(git rev-parse --short HEAD)
VERSION_FILE="version.txt"
CHANGELOG_FILE="CHANGELOG.txt"
DATESTAMP=$(date -u "+%Y%m%d%H%M%S")

rm -rf "${DIST:?}/$RAYDOG"
mkdir -p "$DIST/$RAYDOG"

echo "Commit ID: $COMMIT" > "$DIST/$RAYDOG/$VERSION_FILE"
git log --oneline > "$DIST/$RAYDOG/$CHANGELOG_FILE"

rm -rf raydog/__pycache__ utils/__pycache__

for ITEM in README.md \
            usage-example.py \
            raydog \
            private-key \
            scripts \
            utils \
            CHANGELOG.txt
do
  cp -r $ITEM $DIST/$RAYDOG/.
done

# Temporarily exclude the autoscaler
rm -rf $DIST/$RAYDOG/raydog/autoscaler.py

# Kill macOS detritus
rm -rf $(find . -name '.DS_Store')

chmod 0600 $DIST/$RAYDOG/private-key

cd $DIST || exit
ZIPFILE=raydog-$DATESTAMP-$COMMIT.zip
echo "Creating $ZIPFILE ..."
zip -r $ZIPFILE $RAYDOG
echo "Done"
