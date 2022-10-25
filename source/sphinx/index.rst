.. Licensed to the Apache Software Foundation (ASF) under one or more
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


Welcome to Apache Flume
=======================

Flume is a distributed, reliable, and available service for
efficiently collecting, aggregating, and moving large amounts of log
data. It has a simple and flexible architecture based on streaming
data flows. It is robust and fault tolerant with tunable reliability
mechanisms and many failover and recovery mechanisms.  It
uses a simple extensible data model that allows for online analytic
application.

.. figure:: images/DevGuide_image00.png
   :align: center
   :alt: Agent component diagram

.. rubric:: News

.. raw:: html

   <h3>Oct 24, 2022 - Apache Flume 1.11.0 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.11.0.

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Flume 1.11.0 is stable, production-ready software, and is backwards-compatible with
previous versions of the Flume 1.x codeline.

This version of Flume adds support for deploying Flume as a Spring Boot application, adds support to the
Kafka source and sink for passing the Kafka timestamp and headers, and allows SSL hostname verification
to be disabled in the Kafka source and sink.

Flume 1.11.0 contains a fix for `CVE-2022-42468 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-42468>`__.
See the `Flume Security <./security.html>`__ page for more details.

The full change log and documentation are available on the
`Flume 1.11.0 release page <releases/1.11.0.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

.. raw:: html

   <h3>Aug 16, 2022 - Apache Flume 1.10.1 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.10.1.

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Flume 1.10.1 is stable, production-ready software, and is backwards-compatible with
previous versions of the Flume 1.x codeline.


This version of Flume adds the automatic module name to the manifest of the various Flume
jars allowing them to be usable in applications using the Java Platform Module System.

The full change log and documentation are available on the
`Flume 1.10.1 release page <releases/1.10.1.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

.. raw:: html

   <h3>June 13, 2022 - Apache Flume 1.10.0 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.10.0.

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Flume 1.10.0 is stable, production-ready software, and is backwards-compatible with
previous versions of the Flume 1.x codeline.

Flume 1.10.0 contains a fix for `CVE-2022-25167 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-25167>`__.
See the `Flume Security <./security.html>`__ page for more details.

This version of Flume upgrades many dependencies, resolving the CVEs associated with them.
Enhancements included in this release include the addition of a LoadBalancingChannelSelector,
the ability to retrieve the Flume configuration from a remote source such as a Spring
Cloud Config Server, and support for composite configurations.

Flume has been updated to use Log4j 2.x instead of Log4j 1.x.

The full change log and documentation are available on the
`Flume 1.10.0 release page <releases/1.10.0.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

.. raw:: html

   <h3>January 8, 2019 - Apache Flume 1.9.0 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.9.0.

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Version 1.9.0 is the eleventh Flume release as an Apache top-level project. Flume
1.9.0 is stable, production-ready software, and is backwards-compatible with
previous versions of the Flume 1.x codeline.

Several months of active development went into this release: about 70 patches were committed since 1.8.0, representing many features, enhancements, and bug fixes. While the full change log can be found on the 1.9.0 release page (link below), here are a few new feature highlights:

    * Better SSL/TLS support
    * Configuration Filters to provide a way to inject sensitive information like passwords into the configuration
    * Float and Double value support in Context
    * Kafka client upgraded to 2.0
    * HBase 2 support

Below is the list of people (from Git logs) who submitted and/or reviewed
improvements to Flume during the 1.9.0 development cycle:

    * Andras Beni
    * Attila Simon
    * Bessenyei Balázs Donát
    * Denes Arvay
    * Endre Major
    * Ferenc Szabo
    * Hans Uhlig
    * hunshenshi
    * Jehan Bruggeman
    * Laxman Ch
    * Mike Percy
    * Miklos Csanady
    * Peter Turcsanyi
    * Ralph Goers
    * Takafumi Saito
    * Tristan Stevens
    * Udai Kiran Potluri
    * Viktor Somogyi
    * Yan Jian

The full change log and documentation are available on the
`Flume 1.9.0 release page <releases/1.9.0.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

The Apache Flume Team

.. raw:: html

   <h3>October 4, 2017 - Apache Flume 1.8.0 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.8.0.

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Version 1.8.0 is the eleventh Flume release as an Apache top-level project. Flume
1.8.0 is stable, production-ready software, and is backwards-compatible with
previous versions of the Flume 1.x codeline.

Several months of active development went into this release: about 80 patches were committed since 1.7.0, representing many features, enhancements, and bug fixes. While the full change log can be found on the 1.8.0 release page (link below), here are a few new feature highlights:

    * Add Interceptor to remove headers from event
    * Provide netcat UDP source as alternative to TCP
    * Support environment variables in configuration files

Below is the list of people (from Git logs) who submitted and/or reviewed
improvements to Flume during the 1.8.0 development cycle:

* Andras Beni
* Ashish Paliwal
* Attila Simon
* Ben Wheeler
* Bessenyei Balázs Donát
* Chris Horrocks
* Denes Arvay
* Ferenc Szabo
* Gabriel Commeau
* Hari Shreedharan
* Jeff Holoman
* Lior Zeno
* Marcell Hegedus
* Mike Percy
* Miklos Csanady
* Peter Ableda
* Peter Chen
* Ping Wang
* Pravin D'silva
* Robin Wang
* Roshan Naik
* Satoshi Iijima
* Shang Wu
* Siddharth Ahuja
* Takafumi Saito
* TeddyBear1314
* Theodore michael Malaska
* Tristan Stevens
* dengkai02
* eskrm
* filippovmn
* loleek

The full change log and documentation are available on the
`Flume 1.8.0 release page <releases/1.8.0.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

The Apache Flume Team

.. raw:: html

   <h3>October 17, 2016 - Apache Flume 1.7.0 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.7.0.

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Version 1.7.0 is the tenth Flume release as an Apache top-level project.
Flume 1.7.0 is stable, production-ready software, and is backwards-compatible
with previous versions of the Flume 1.x codeline.

Several months of active development went into this release: almost 100 patches were committed since 1.6.0, representing many features, enhancements, and bug fixes. While the full change log can be found on the 1.7.0 release page (link below), here are a few new feature highlights:

    * Taildir source
    * Kafka integration improvements (eg. security)

Below is the list of people (from Git/SVN logs) who submitted and/or reviewed
improvements to Flume during the 1.7.0 development cycle:

* Abraham Fine
* Alexandre Dutra
* Andrea Rota
* Ashish Paliwal
* Attila Simon
* Bessenyei Balázs Donát
* Daniel Templeton
* Deepesh Khandelwal
* Denes Arvay
* Dylan Jones
* Fonso Li
* Gonzalo Herreros
* Grant Henke
* Grigoriy Rozhkov
* Hari Shreedharan
* Neerja Khattar
* Jarek Jarcec Cecho
* Jeff Holoman
* Johny Rufus
* Jonathan Smith
* Jun Seok Hong
* Kevin Conaway
* lfzCarlosC
* Li Xiang
* Liam Mousseau
* Lionel Herbet
* Lior Zeno
* Michelle Casbon
* Mike Percy
* Miroslav Holubec
* Muhammad Ehsan ul Haquen
* Niccolo Becchi
* Phil D'Amore
* Phil Scalan
* Philip Zeyliger
* Ralph Goer
* Rigo MacTaggart
* Roshan Naik
* Santiago M. Mola
* Satoshi Iijima
* Siddharth Ahuja
* Sriharsha Chintalapani
* Somin Mithraa
* Sriharsha Chintalapani
* Ted Malaska
* tinawenqiao
* Tristan Stevens

The full change log and documentation are available on the
`Flume 1.7.0 release page <releases/1.7.0.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

The Apache Flume Team


.. raw:: html

   <h3>May 20, 2015 - Apache Flume 1.6.0 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.6.0.

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Version 1.6.0 is the ninth Flume release as an Apache top-level project.
Flume 1.6.0 is stable, production-ready software, and is backwards-compatible
with previous versions of the Flume 1.x codeline.

Several months of active development went into this release: 105 patches were committed since 1.5.2, representing many features, enhancements, and bug fixes. While the full change log can be found on the 1.6.0 release page (link below), here are a few new feature highlights:

    * Flume Sink and Source for Apache Kafka
    * A new channel that uses Kafka
    * Hive Sink based on the new Hive Streaming support
    * End to End authentication in Flume
    * Simple regex search-and-replace interceptor

The full change log and documentation are available on the
`Flume 1.6.0 release page <releases/1.6.0.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

The Apache Flume Team

.. raw:: html

   <h3>November 18, 2014 - Apache Flume 1.5.2 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.5.2

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Version 1.5.2 is the eighth Flume release as an Apache top-level project.
Flume 1.5.2 is stable, production-ready software, and is backwards-compatible
with previous versions of the Flume 1.x codeline.

Apache Flume 1.5.2 is a security and maintenance release that disables SSLv3
on all components in Flume that support SSL/TLS. All users are encouraged to
update to this release as soon as possible.

The full change log and documentation are available on the
`Flume 1.5.2 release page <releases/1.5.2.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

The Apache Flume Team

.. raw:: html

   <h3>July 16, 2014 - Apache Flume 1.5.0.1 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.5.0.1

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Version 1.5.0.1 is the sixth Flume release as an Apache top-level project.
Flume 1.5.0.1 is stable, production-ready software, and is backwards-compatible
with previous versions of the Flume 1.x codeline.

Apache Flume 1.5.0.1 is a maintenance release primarily meant to add support to build
against Apache HBase 0.98.x. This release adds a new build profile that builds Flume
against HBase 0.98.2.

Apache BigTop 0.8.0 release will ship Flume binaries built
against HBase 0.98.x.

The full change log and documentation are available on the
`Flume 1.5.0.1 release page <releases/1.5.0.1.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

The Apache Flume Team

.. raw:: html

   <h3>May 20, 2014 - Apache Flume 1.5.0 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.5.0.

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Version 1.5.0 is the fifth Flume release as an Apache top-level project.
Flume 1.5.0 is stable, production-ready software, and is backwards-compatible
with previous versions of the Flume 1.x codeline.

Several months of active development went into this release: 123 patches were committed since 1.4.0, representing many features, enhancements, and bug fixes. While the full change log can be found on the 1.5.0 release page (link below), here are a few new feature highlights:

* New in-memory channel that can spill to disk
* A new dataset sink that use Kite API to write data to HDFS and HBase
* Support for Elastic Search HTTP API in Elastic Search Sink
* Much faster replay in the File Channel.

The full change log and documentation are available on the
`Flume 1.5.0 release page <releases/1.5.0.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

The Apache Flume Team


.. raw:: html

   <h3>July 2, 2013 - Apache Flume 1.4.0 Released</h3>

The Apache Flume team is pleased to announce the release of Flume 1.4.0.

Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of streaming event data.

Version 1.4.0 is the fourth Flume release as an Apache top-level project.
Flume 1.4.0 is stable, production-ready software, and is backwards-compatible
with previous versions of the Flume 1.x codeline.

Six months of active development went into this release: 261 patches were committed since 1.3.1, representing many features, enhancements, and bug fixes. While the full change log can be found on the 1.4.0 release page (link below), here are a few new feature highlights:

* New JMS Source
* New Solr Sink with ETL capabilities
* Updated ElasticSearch sink to support ES version 0.90
* Support for secure SSL transport over Avro-RPC clients, sources & sinks
* Support for Thrift-RPC as a transport mechanism
* Support for embedding a Flume agent within applications
* Support for a new plugins.d directory structure for managing Flume addons
* Support for reading Avro files via the Spooling Directory source
* Support for writing Avro files with arbitrary schemas via the HDFS sink
* Support for ingesting Avro-serializable objects via the log4j API
* Improvements to the file channel to keep a backup checkpoint to avoid replays
* Performance improvements to the file channel, including group commit
* New file channel consistency check tool

Below is the list of people (from Git/SVN logs) who submitted and/or reviewed
improvements to Flume during the 1.4.0 development cycle:

* Alexander Alten-Lorenz
* Aline Guedes Pinto
* Brock Noland
* Cameron Gandevia
* Chris Birchall
* Christopher Nagy
* Deepesh Khandelwal
* Denny Ye
* Edward Sargisson
* Hari Shreedharan
* Israel Ekpo
* Ivan Bogdanov
* Jarek Jarcec Cecho
* Jeff Lord
* Joey Echeverria
* Jolly Chen
* Juhani Connolly
* Mark Grover
* Mike Percy
* Mubarak Seyed
* Nitin Verma
* Oliver B. Fischer
* Patrick Wendell
* Paul Chavez
* Pedro Urbina Escos
* Phil Scala
* Rahul Ravindran
* Ralph Goers
* Roman Shaposhnik
* Roshan Naik
* Sravya Tirukkovalur
* Steve Hoffman
* Ted Malaska
* Thiruvalluvan M. G.
* Thom DeCarlo
* Tim Bacon
* Tom White
* Venkat Ranganathan
* Venkatesh Sivasubramanian
* Will McQueen
* Wolfgang Hoschek

The full change log and documentation are available on the
`Flume 1.4.0 release page <releases/1.4.0.html>`__.

This release can be downloaded from the Flume `Download <download.html>`__ page.

Your contributions, feedback, help and support make Flume better!
For more information on how to report problems or contribute,
please visit our `Get Involved <getinvolved.html>`__ page.

The Apache Flume Team


.. raw:: html

   <h3>January 2, 2013 - Apache Flume 1.3.1 Released</h3>

The Apache Flume team is pleased to announce the release of Flume version 1.3.1.
Apache Flume 1.3.1 is the fifth release under the auspices of Apache of the
so-called "NG" codeline, and our third release as a top-level Apache
project! Flume 1.3.1 has been put through many stress and regression tests,
is stable, production-ready software, and is backwards-compatible with
Flume 1.3.0 and Flume 1.2.0.

Apache Flume 1.3.1 is a maintainance release for the 1.3.0 release, and includes
several bug fixes and performance enhancements.

This release can be downloaded from the Flume download page at:
http://flume.apache.org/download.html

The change log and documentation are available on the 1.3.1 release page:
http://flume.apache.org/releases/1.3.1.html

Your help and feedback is more than welcome!

.. raw:: html

   <h3>December 2, 2012 - Apache Flume 1.3.0 Released</h3>

The Apache Flume team is pleased to announce the release of Flume version 1.3.0.

Apache Flume 1.3.0 is the fourth release under the auspices of Apache of the
so-called "NG" codeline, and our second release as a top-level Apache
project! Flume 1.3.0 has been put through many stress and regression tests,
is stable, production-ready software, and is backwards-compatible with
Flume 1.2.0.

Four months of very active development went into this release: a whopping
221 patches were committed since 1.2.0, representing many features,
enhancements, and bug fixes. While the full change log can be found in the
link below, here are a few new feature highlights:

* New HTTP Post Source
* New Spool Directory Source
* New Multi-port Syslog Source
* New Elastic Search Sink
* New Regex Extractor Interceptor
* File Channel Encryption

This release can be downloaded from the Flume download page at:
http://flume.apache.org/download.html

The change log and documentation are available on the 1.3.0 release page:
http://flume.apache.org/releases/1.3.0.html

Your help and feedback is more than welcome!

.. raw:: html

   <h3>July 26, 2012 - Apache Flume 1.2.0 Released</h3>

The Apache Flume team is pleased to announce the release of Flume version 1.2.0.

Apache Flume 1.2.0 is the third release under the auspices of Apache of the
so-called "NG" codeline, and our first release as a top-level Apache
project! Flume 1.2.0 has been put through many stress and regression tests,
is stable, production-ready software, and is backwards-compatible with
Flume 1.1.0.

Four months of very active development went into this release: a whopping
192 patches were committed since 1.1.0, representing many features,
enhancements, and bug fixes. While the full change log can be found in the
link below, here are a few new feature highlights:

* New durable file channel
* New client API
* New HBase sinks (two different implementations)
* New Interceptor interface (a plugin processing API)
* New JMX-based monitoring support

This release can be downloaded from the Flume download page at:
http://flume.apache.org/download.html

The change log and documentation are available on the 1.2.0 release page:
http://flume.apache.org/releases/1.2.0.html

Your help and feedback is more than welcome!

.. raw:: html

   <h3>July 26, 2012 - Flume Meetup NYC</h3>

Strata + Hadoop World (http://strataconf.com/stratany2012) will be
held October 23-25 in NYC. A Flume Meetup is being organized
around this event. This Meetup will be a good place for users to
interact with each other and with Flume developers.

In order to help with organization, a `form <https://docs.google.com/spreadsheet/viewform?formkey=dGRDUUxxbUI3TUJuYnQwMV9sVzJDZEE6MA#gid=0>`__
has been set up with a few questions about what kind of Meetup the community wants, and which evening is best.
Please fill out the `form <https://docs.google.com/spreadsheet/viewform?formkey=dGRDUUxxbUI3TUJuYnQwMV9sVzJDZEE6MA#gid=0>`__.
Feel free to post to the User's mailing list with any questions.

.. toctree::
   :maxdepth: 1
   :hidden:

   getinvolved
   download
   security
   documentation
   releases/index
   mailinglists
   team
   source
   testing
   license

..
