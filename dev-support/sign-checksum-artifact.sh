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

usage() {
  echo "Usage: $0 RELEASE_ARTIFACT" 1>&2
  echo "Example: $0 ./apache-flume-1.7.0-bin.tar.gz" 1>&2
  exit 1
}

error() {
  echo $1 1>&2
  exit 1
}

ARTIFACT=$1
if [ ! -r "$ARTIFACT" ]; then
  echo "The artifact at $ARTIFACT does not exist or is not readable." 1>&2
  usage
fi

# Find GnuPG.
GPG=$(which gpg)
[ -z "$GPG" ] && error "Cannot find gpg. Please install GnuPG to continue."

# Find md5.
MD5=$(which md5sum)
[ -z "$MD5" ] && MD5=$(which md5)
[ -z "$MD5" ] && error "Cannot find md5sum. Please install the md5sum program to continue."

# Find sha1.
SHA1=$(which sha1sum)
[ -z "$SHA1" ] && SHA1=$(which shasum)
[ -z "$SHA1" ] && error "Cannot find sha1sum. Please install the sha1sum program to continue."

# Now sign and checksum the artifact.
set -x
$GPG --sign $ARTIFACT
$MD5 < $ARTIFACT > $ARTIFACT.md5
$SHA1 < $ARTIFACT > $ARTIFACT.sha1

exit 0
