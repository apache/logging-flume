# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

ARG IMAGE_NAME=adoptopenjdk/openjdk8
ARG IMAGE_TAG=centos-jre

FROM ${IMAGE_NAME}:${IMAGE_TAG}

ARG ASSEMBLY_VERSION
ARG MAINTAINER="Apache Flume <dev@flume.apache.org>"

LABEL maintainer="${MAINTAINER}"
LABEL site="https://flume.apache.org"

COPY target/apache-flume-${ASSEMBLY_VERSION}-bin/apache-flume-${ASSEMBLY_VERSION}-bin/ /etc/flume/

RUN echo $ASSEMBLY_VERSION

RUN groupadd --system flume && useradd --system --shell /bin/false -g flume flume

WORKDIR /etc/flume
RUN chown -R flume:flume /etc/flume
USER flume
ENTRYPOINT ["/etc/flume/bin/flume-ng"]
