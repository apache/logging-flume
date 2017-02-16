#!/bin/bash -e
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
################################################################################
# Script to generate and sign a source release.
################################################################################
DEV_SUPPORT=$(cd $(dirname $0); pwd)
source "$DEV_SUPPORT/includes.sh"

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
CREATE_ARCHIVE=$DEV_SUPPORT/create-source-archive.sh
ARCHIVE_PATH=$($CREATE_ARCHIVE "$VERSION_NUMBER" "$GIT_TAG" "$OUTPUT_DIR")
[ $? != 0 ] && error "Failed to generate source archive. $CREATE_ARCHIVE returned $?"
[ ! -r $ARCHIVE_PATH ] && error "Failed to generate source archive. Unknown error."

# Sign and checksum the source artifact.
echo "Signing source artifact..."
SIGN_ARTIFACT=$DEV_SUPPORT/sign-checksum-artifact.sh
$SIGN_ARTIFACT "$ARCHIVE_PATH"

echo "Release artifacts generated in $OUTPUT_DIR"
exit 0

