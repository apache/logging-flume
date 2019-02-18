#!/usr/bin/env bash
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
# Script to compare LICENSE file content and distributed jars
# use after flume-ng-dist target has been generated (mvn clean package)
################################################################################
PROJECT_ROOT="$(cd $(dirname $0)/..; pwd)"

echo "LICENSE file <---------> jar files"
diff -y <(grep .jar: ${PROJECT_ROOT}/LICENSE | sed -E 's/:|\s+//g' | sort) <(ls ${PROJECT_ROOT}/flume-ng-dist/target/apache-flume-*-bin/apache-flume-*-bin/lib/*.jar |xargs -n1 basename |grep -v flume | sed -E 's/(.+)-([^-]+).jar/\1-<version>.jar/' | uniq | sort)
