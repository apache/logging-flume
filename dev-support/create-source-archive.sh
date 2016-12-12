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
# Script to generate a source release tarball.
# The contract of this script is that it outputs the location of the generated
# tarball to stdout upon successful completion.
################################################################################
DEV_SUPPORT=$(cd $(dirname $0); pwd)
source "$DEV_SUPPORT/includes.sh"

VERSION_NUMBER=$1
GIT_TAG=$2
OUTPUT_DIR=$3

if [[ -z "$VERSION_NUMBER" || -z "$GIT_TAG" || -z "$OUTPUT_DIR" ]]; then
  echo "Usage: $0 VERSION_NUMBER GIT_TAG OUTPUT_DIR" 1>&2
  echo "Example: $0 1.7.0 release-1.7.0-rc1 target" 1>&2
  exit 1
fi

[ ! -d "$OUTPUT_DIR" ] && error "Output directory $OUTPUT_DIR does not exist."
ABS_OUTPUT_DIR=$(cd $OUTPUT_DIR; pwd)

EXT=tar.gz
ARTIFACT_NAME=apache-flume-${VERSION_NUMBER}-src
ARTIFACT_PATH=$ABS_OUTPUT_DIR/$ARTIFACT_NAME.$EXT

# Need to call git archive from the root of the tree.
cd $DEV_SUPPORT/..

echo git archive --prefix=$ARTIFACT_NAME/ --output=$ARTIFACT_PATH --format "$EXT" "$GIT_TAG" 1>&2
git archive --prefix=$ARTIFACT_NAME/ --output=$ARTIFACT_PATH --format "$EXT" "$GIT_TAG"

echo $ARTIFACT_PATH
exit 0
