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
# Utilities for dev-support scripts.
################################################################################

# Print an error message and exit.
error() {
  echo $1 1>&2
  exit 1
}

# Searches the PATH for each command name passed, and returns the path of the
# first one found.
find_in_path() {
  for COMMAND in "$@"; do
    FOUND=$(which $COMMAND)
    if [ -n "$FOUND" ]; then
      echo "$FOUND"
      return
    fi
  done
  error "Cannot find $1. Please install $1 to continue."
}
