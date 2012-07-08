#!/bin/bash 

# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# extract the bodies of the avro data.

# sed extract body
# sort sorts
# awk points out start, stops, and sequence discontinuities

sed 's/.*"body":"\(.*\)"."timestamp".*/\1/' | sort | uniq -c | awk "{ if (NR==1) print \"start:\t\"\$2,\$3,\$1}  {if (FNR!=\$2) print \"discontinuity:\", FNR, \$2, \$4 ; FNR=\$2 +0 } {if (\$1!=1) print \"dupes:\t\", \$2,\$3,\$1 } END { print\"end:\t\",\$2,\$3,\$1}"
