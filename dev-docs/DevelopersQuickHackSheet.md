<!---
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
Apache Flume: Developers Quick Hack Sheet
=========================================

Developers Quick Hack Sheet
===========================

### Get the Code


```
git clone https://github.com/apache/flume.git flume-local
cd flume-local
git checkout trunk
```


for a particular release:


```
git clone https://github.com/apache/flume.git flume-local
cd flume-local
git checkout <RELEASE>
```


-   If you want to fix a issue, grab the Jira
    (<https://issues.apache.org/jira/browse/FLUME>) you want to fix
-   If you have a new feature / improvement, open a new Jira with the
    Tag "Improvement" / "Feature"

### Work With Your Code

-   Make your code changes
-   Test. Test again and we forget to mention: Test
-   If your code change works make a patch


```
git diff --no-prefix > /Path/to/your/patch/JIRA-ID.patch
```


-   If you want to work at other patches, go and stash your work:


```
git stash (will save your branch and reset the working directory)
```


-   Attach the JIRA-ID.patch to the Jira



-   Open a review request at <https://reviews.apache.org> against
    flume-git
    -   Fill in the field Bug-ID the Jira-ID to link both together
    -   Explain your code
    -   Add unit tests
        -   **please note, patches without a working unit test will be
            rejected and not commited**
    -   fill out all the fields
    -   Check your Diff by clicking "View Diff"
        -   **if you see some red fields, check your syntax and fix this
            please**
    -   review your request
    -   publish



-   Post the review link into the Jira
-   Write Notes into the Jira, regarding your work

### Known Build Issues


```
Exception in thread "MainThread" java.lang.OutOfMemoryError: PermGen space
```


Maven2 Issue, start your build with:


```
MAVEN_OPTS="-Xms512m -Xmx1024m -XX:PermSize=256m -XX:MaxPermSize=512m" mvn package -DskipTests
```


Jira:
[FlUME-1256](https://issues.apache.org/jira/browse/FLUME-1256)

Thank you for contributing Flume!
=================================
