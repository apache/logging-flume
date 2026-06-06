<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Overview

This is a walk through for verifying and updating the LICENSE/NOTICE files for
our project.

# Source

The top of the repository should have a LICENSE/NOTICE combination that covers
the _source_ of the project. It should not include any information for the
binary artifacts.

Note that if some third party source code is included in our repository you
should also ensure that whichever binary artifacts are generated from the
source are also updated to include LICENSE/NOTICE changes.

Currently our only bundled third party sources are some test files for the SOLR
sink. The files are covered under the 2-Clause BSD license, so they must be
noted in our top level LICENSE and in the LICENSE file included in the
`flume-ng-morphline-solr-sink` module's `test-jar` artifact.

# Verify NOTICE copyright year

Ensure our top level pom defines both a top level `inceptionYear` element and a `project.build.outputTimestamp` property that is set to the year for the creating the release. These two values are used by the build tooling to set an appropriate copyright notice on our generated artifacts in their respective NOTICE files. Note that during release candidate building the `maven-release-plugin` should take care of updating the project's build output timestamp.

# Jars with bundled dependencies

Any generated jars that include third party dependencies via the build process will need to be checked for appropriately maintaining LICENSE/NOTICE files.

You can generate a current alphabetized list of the bundled dependencies with
these steps for a given module:

```bash
% mvn -pl flume-ng-clients/flume-ng-log4jappender dependency:list -DincludeScope=runtime -DoutputFile=dep-list.out
% cat flume-ng-clients/flume-ng-log4jappender/dep-list.out | tail -n +3 | sort -t: -k2 > flume-ng-clients/flume-ng-log4jappender/target/flume-ng-log4jappender-deplist.out
```

Use this list to compare to the following list of artifacts and make any needed
changes to the LICENSE and NOTICE files in the module's directory
`src/main/appended-resources/META-INF/`. These files do not need to include the
ALv2 license nor the boilerplate module name and copyright statement because
the build will fill them in.


# Binary distro

You can generate a current alphabetized list of the bundled dependencies with
these steps (be sure to substitute the project version for `<version>` below):

```bash
% ls -1 flume-ng-dist/target/apache-flume-<version>-bin/apache-flume-<version>-bin/lib/ \
    flume-ng-dist/target/apache-flume-<version>-bin/apache-flume-<version>-bin/tools \
    | sort > flume-ng-dist/target/flume-ng-dist-deplist.out
```

Use this list to compare to the following list of artifacts and make any needed
changes to the NOTICE and LICENSE files in the module's directory
`src/main/appended-resources/META-INF/`. This file do not need to include the
ALv2 license because the build will fill it in.

Note that where appropriate this list says "same as jar-with-dependencies" for
information that should be copied from a the jars with bundled dependencies
above.

```
   async-1.4.0.jar
```

3-Clause BSD. Entry in License.

```
   asynchbase-1.8.2.jar
```

3-Clause BSD. Entry in License.

```
   audience-annotations-0.5.0.jar
```

ALv2. Entry in NOTICE.

```
   avro-1.11.0.jar
   avro-ipc-1.11.0.jar
   avro-ipc-jetty-1.11.0.jar
   avro-ipc-netty-1.11.0.jar
```

same as jar-with-dependencies

```
   commons-cli-1.5.0.jar
```

ALv2. Entry in NOTICE.

```
   commons-codec-1.15.jar
```

same as jar-with-dependencies

```
   commons-collections-3.2.2.jar
```

ALv2. Entry in NOTICE.

```
   commons-compress-1.21.jar
```

same as jar-with-dependencies

```
   commons-dbcp-1.4.jar
```

ALv2. Entry in NOTICE.

```
   commons-io-2.11.0.jar
```

ALv2. Entry in NOTICE.

```
   commons-lang-2.6.jar
```

same as jar-with-dependencies

```
   commons-lang3-3.11.jar
```

same as jar-with-dependencies

```
   commons-logging-1.2.jar
```

same as jar-with-dependencies

```
   commons-pool-1.5.4.jar
```

ALv2. Entry in NOTICE.

```
   commons-text-1.9.jar
```

ALv2. Entry in NOTICE.

```
   curator-client-5.1.0.jar
   curator-framework-5.1.0.jar
   curator-recipes-5.1.0.jar
```

ALv2. Entry in NOTICE

```
   derby-10.14.1.0.jar
```

ALv2. Entry in NOTICE. Additional entry for IBM copyrights. Remainder of the
derby NOTICE is not relevant.

```
   flume-avro-source-<version>.jar
   flume-file-channel-<version>.jar
   flume-hdfs-sink-<version>.jar
   flume-hive-sink-<version>.jar
   flume-http-sink-<version>.jar
   flume-irc-sink-<version>.jar
   flume-jdbc-channel-<version>.jar
   flume-jms-source-<version>.jar
   flume-kafka-channel-<version>.jar
   flume-kafka-source-<version>.jar
   flume-ng-auth-<version>.jar
   flume-ng-config-filter-api-<version>.jar
   flume-ng-configuration-<version>.jar
   flume-ng-core-<version>.jar
   flume-ng-embedded-agent-<version>.jar
   flume-ng-environment-variable-config-filter-<version>.jar
   flume-ng-external-process-config-filter-<version>.jar
   flume-ng-hadoop-credential-store-config-filter-<version>.jar
   flume-ng-hbase-sink-<version>.jar
   flume-ng-hbase2-sink-<version>.jar
   flume-ng-kafka-sink-<version>.jar
   flume-ng-log4jappender-<version>-jar-with-dependencies.jar
   flume-ng-log4jappender-<version>.jar
   flume-ng-morphline-solr-sink-<version>.jar
   flume-ng-node-<version>.jar
   flume-ng-sdk-<version>.jar
   flume-scribe-source-<version>.jar
   flume-shared-kafka-<version>.jar
   flume-spillable-memory-channel-<version>.jar
   flume-taildir-source-<version>.jar
   flume-thrift-source-<version>.jar
   flume-tools-<version>.jar
   flume-twitter-source-<version>.jar
```

ALv2. Entry in NOTICE.

```
   gson-2.2.2.jar
```

ALv2. No NOTICE.

```
   guava-11.0.2.jar
```

ALv2. No NOTICE.

```
   httpclient-4.5.13.jar
```

same as jar-with-dependencies

```
   httpcore-4.4.15.jar
```

same as jar-with-dependencies

```
   irclib-1.10.jar
```

ALv2. No NOTICE.

```
   jackson-annotations-2.13.2.jar
   jackson-core-2.13.2.jar
   jackson-databind-2.13.2.1.jar
   jackson-dataformat-csv-2.10.5.jar
   jackson-datatype-jdk8-2.10.5.jar
```

same as jar-with-dependencies

```
   javax.annotation-api-1.3.2.jar
```

same as jar-with-dependencies

```
   javax.servlet-api-3.1.0.jar
```

CDDL v1.0. Entry in LICENSE and NOTICE.


```

BSD-like project specific. Entry in LICENSE and NOTICE.

```
   jetty-http-9.4.51.v20230217.jar
   jetty-io-9.4.51.v20230217.jar
   jetty-jmx-9.4.51.v20230217.jar
   jetty-security-9.4.51.v20230217.jar
   jetty-server-9.4.51.v20230217.jar
   jetty-servlet-9.4.51.v20230217.jar
   jetty-util-9.4.51.v20230217.jar
   jetty-util-9.4.51.v20230217.jar
```

ALv2. Entry in NOTICE. Additional entry for UnixCrypt.

```

ALv2. Entry in NOTICE.

```
   jopt-simple-5.0.4.jar
```

MIT. Entry in LICENSE.

```
   jsr305-1.3.9.jar
```

BSD 3-Clause. Entry in LICENSE.  Artifact is the JS305 RI published by
Findbugs.
* build: https://github.com/findbugsproject/findbugs/blob/1.3.9/findbugs/build.xml#L1072
* RI: https://code.google.com/archive/p/jsr-305/

```
   jzlib-1.1.3.jar
```

same as jar-with-dependencies

```
   kafka-clients-3.3.1.jar
   kafka-raft-3.3.1.jar
   kafka_2.13-3.3.1.jar
```

ALv2. Entry in NOTICE.  Additional entry in NOTICE and LICENSE for
PureJavaCrc32C; portions 2-Clause BSD.

```
   libthrift-0.14.2.jar
```

same as jar-with-dependencies

```
   log4j-1.2-api-2.17.1.jar
   log4j-api-2.17.1.jar
   log4j-core-2.17.1.jar
   log4j-slf4j-impl-2.17.1.jar
```

ALv2. Entry in NOTICE. Additional entries for ResolverUtil, TypeUtil, PicoCLI.

```
   lz4-java-1.7.1.jar
```

ALv2. No NOTICE.

```
   mapdb-0.9.9.jar
```

ALv2. No NOTICE.

```
   metrics-core-2.2.0.jar
```

ALv2. Entry in NOTICE. LICENSE entry for JSR-166 in Public Domain.

```
   metrics-core-4.1.18.jar
```

ALv2. No NOTICE.

```
   mina-core-2.1.5.jar
```

ALv2. Entry in NOTICE.

```
   netty-3.9.4.Final.jar
   netty-all-4.1.72.Final.jar
   netty-buffer-4.1.68.Final.jar
   netty-codec-4.1.72.Final.jar
   netty-codec-dns-4.1.72.Final.jar
   netty-codec-haproxy-4.1.72.Final.jar
   netty-codec-http-4.1.72.Final.jar
   netty-codec-http2-4.1.72.Final.jar
   netty-codec-memcache-4.1.72.Final.jar
   netty-codec-mqtt-4.1.72.Final.jar
   netty-codec-redis-4.1.72.Final.jar
   netty-codec-smtp-4.1.72.Final.jar
   netty-codec-socks-4.1.72.Final.jar
   netty-codec-stomp-4.1.72.Final.jar
   netty-codec-xml-4.1.72.Final.jar
   netty-common-4.1.72.Final.jar
   netty-handler-4.1.68.Final.jar
   netty-handler-proxy-4.1.72.Final.jar
   netty-resolver-4.1.72.Final.jar
   netty-resolver-dns-4.1.72.Final.jar
   netty-resolver-dns-classes-macos-4.1.72.Final.jar
   netty-resolver-dns-native-macos-4.1.72.Final-osx-aarch_64.jar
   netty-resolver-dns-native-macos-4.1.72.Final-osx-x86_64.jar
   netty-tcnative-classes-2.0.46.Final.jar
   netty-transport-4.1.72.Final.jar
   netty-transport-classes-epoll-4.1.72.Final.jar
   netty-transport-classes-kqueue-4.1.72.Final.jar
   netty-transport-native-epoll-4.1.50.Final.jar
   netty-transport-native-epoll-4.1.72.Final-linux-aarch_64.jar
   netty-transport-native-epoll-4.1.72.Final-linux-x86_64.jar
   netty-transport-native-kqueue-4.1.72.Final-osx-aarch_64.jar
   netty-transport-native-kqueue-4.1.72.Final-osx-x86_64.jar
   netty-transport-native-unix-common-4.1.72.Final.jar
   netty-transport-rxtx-4.1.72.Final.jar
   netty-transport-sctp-4.1.72.Final.jar
   netty-transport-udt-4.1.72.Final.jar
```

same as jar-with-dependencies, additional copyright year for netty-3.9.4.

```
   protobuf-java-2.5.0.jar
```

3-Clause BSD. Entry in LICENSE.

```
   slf4j-api-1.7.32.jar
```

same as jar-with-dependencies

```
   snappy-java-1.1.8.4.jar
```

same as jar-with-dependencies

```
   tomcat-annotations-api-8.5.46.jar
   tomcat-embed-core-8.5.46.jar
```

same as jar-with-dependencies


```
   velocity-engine-core-2.3.jar
```

same as jar-with-dependencies

```
   xz-1.9.jar
```

same as jar-with-dependencies

```
   zookeeper-3.6.2.jar
   zookeeper-jute-3.6.2.jar
```

ALv2. Entry in NOTICE. Additional entry for Airlift.

```
   zstd-jni-1.4.5-6.jar
```

same as jar-with-dependencies

