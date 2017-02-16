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
# Sign and checksum release artifacts.
################################################################################
DEV_SUPPORT=$(cd $(dirname $0); pwd)
source "$DEV_SUPPORT/includes.sh"

usage() {
  echo "Usage: $0 RELEASE_ARTIFACT" 1>&2
  echo "Example: $0 ./apache-flume-1.7.0-src.tar.gz" 1>&2
  exit 1
}

ARTIFACT=$1
if [ ! -r "$ARTIFACT" ]; then
  echo "The artifact at $ARTIFACT does not exist or is not readable." 1>&2
  usage
fi

# The tools we need.
GPG=$(find_in_path gpg)
MD5=$(find_in_path md5sum md5)
SHA1=$(find_in_path sha1sum shasum)

# Now sign and checksum the artifact.
set -x
$GPG --sign $ARTIFACT
$MD5 < $ARTIFACT > $ARTIFACT.md5
$SHA1 < $ARTIFACT > $ARTIFACT.sha1
