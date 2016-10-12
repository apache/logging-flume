#!/bin/bash -e
################################################################################
# Script to generate and sign a source release.
################################################################################

error() {
  echo $1 1>&2
  exit 1
}

ROOT=$(cd $(dirname $0); pwd)
VERSION_NUMBER=$1
GIT_TAG=$2
OUTPUT_DIR=$3

if [[ -z "$VERSION_NUMBER" || -z "$GIT_TAG" || -z "$OUTPUT_DIR" ]]; then
  echo "Usage: $0 VERSION_NUMBER GIT_TAG OUTPUT_DIR"
  echo "Example: $0 1.7.0 release-1.7.0-rc1 target"
  exit 1
fi

# Generate the source artifact.
echo "Creating source archive..."
CREATE_ARCHIVE=$ROOT/create-source-archive.sh
ARCHIVE_PATH=$($CREATE_ARCHIVE "$VERSION_NUMBER" "$GIT_TAG" "$OUTPUT_DIR")
[ $? != 0 ] && error "Failed to generate source archive. $CREATE_ARCHIVE returned $?"
[ ! -r $ARCHIVE_PATH ] && error "Failed to generate source archive. Unknown error."

# Sign and checksum the source artifact.
echo "Signing source artifact..."
SIGN_ARTIFACT=$ROOT/sign-checksum-artifact.sh
$SIGN_ARTIFACT "$ARCHIVE_PATH"

echo "Release artifacts generated in $OUTPUT_DIR"
exit 0

