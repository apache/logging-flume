========
Download
========

Apache Flume is distributed under the `Apache License, version 2.0 <http://www.apache.org/licenses/LICENSE-2.0.html>`_

The link in the Mirrors column should display a list of available mirrors with a default selection based on your
inferred location. If you do not see that page, try a different browser. The checksum and signature are links to the
originals on the main distribution server.

.. csv-table::

   "Apache Flume binary (tar.gz)",  `apache-flume-1.11.0-bin.tar.gz <http://www.apache.org/dyn/closer.lua/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz>`_, `apache-flume-1.11.0-bin.tar.gz.sha512 <http://www.apache.org/dist/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz.sha512>`_, `apache-flume-1.11.0-bin.tar.gz.asc <http://www.apache.org/dist/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz.asc>`_
  "Apache Flume source (tar.gz)",  `apache-flume-1.11.0-src.tar.gz <http://www.apache.org/dyn/closer.lua/flume/1.11.0/apache-flume-1.11.0-src.tar.gz>`_, `apache-flume-1.11.0-src.tar.gz.sha512 <http://www.apache.org/dist/flume/1.11.0/apache-flume-1.11.0-src.tar.gz.sha512>`_, `apache-flume-1.11.0-src.tar.gz.asc <http://www.apache.org/dist/flume/1.11.0/apache-flume-1.11.0-src.tar.gz.asc>`_

It is essential that you verify the integrity of the downloaded files using the PGP or SHA512 signatures. Please read
`Verifying Apache HTTP Server Releases <http://httpd.apache.org/dev/verification.html>`_ for more information on
why you should verify our releases.

The PGP signatures can be verified using PGP or GPG. First download the `KEYS <http://www.apache.org/dist/flume/KEYS>`_
as well as the asc signature file for the relevant distribution. Make sure you get these files from the
`main distribution directory <http://www.apache.org/dist/flume/>`_ rather than from a mirror.
Then verify the signatures using::

    % gpg --import KEYS
    % gpg --verify apache-flume-1.11.0-src.tar.gz.asc

Apache Flume 1.11.0 is signed by Ralph Goers B3D8E1BA

In addition, you can verify the SHA512 checksum on the files. A Unix program called sha or sha512sum is included in
many Unix distributions. Note that verifying the checksum is unnecessary if the PGP signature has been validated.

.. rubric:: Previous_Releases

All previous releases of Apache Flume can be found in the `archive repository <http://archive.apache.org/dist/flume>`_.
