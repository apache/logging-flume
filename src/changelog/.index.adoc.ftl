////
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
////

////
    ██     ██  █████  ██████  ███    ██ ██ ███    ██  ██████  ██
    ██     ██ ██   ██ ██   ██ ████   ██ ██ ████   ██ ██       ██
    ██  █  ██ ███████ ██████  ██ ██  ██ ██ ██ ██  ██ ██   ███ ██
    ██ ███ ██ ██   ██ ██   ██ ██  ██ ██ ██ ██  ██ ██ ██    ██
     ███ ███  ██   ██ ██   ██ ██   ████ ██ ██   ████  ██████  ██

    IF THIS FILE IS CALLED `release-notes.adoc`, IT IS AUTO-GENERATED, DO NOT EDIT IT!

    The release notes page is generated from `src/changelog/.index.adoc.ftl` during
    the `pre-site` phase of the Maven build and is written to
    `target/generated-site/antora/modules/ROOT/pages/release-notes.adoc`,
    where the Antora site build picks it up.
    Hence, you must always edit `.index.adoc.ftl` and never the generated file.
////

// Release notes index does not look nice with a deep sectioning, override it:
:page-toclevels: 1

${"[#release-notes]"}
= Release notes
<#list releases as release><#if release.changelogEntryCount gt 0>

include::_release-notes/${release.version}.adoc[]
</#if></#list>
