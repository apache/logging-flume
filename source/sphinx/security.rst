=====================================
Apache Flume Security Vulnerabilities
=====================================

This page lists all the security vulnerabilities fixed in released versions of Apache Flume. Each vulnerability is given a security impact rating by the Apache Flume security team. Note that this rating may vary from platform to platform. We also list the versions of Apache Flume the flaw is known to affect, and where a flaw has not been verified list the version with a question mark.

Binary patches are never provided. If you need to apply a source code patch, use the building instructions for the Apache Flume version that you are using.

If you need help on building or configuring Flume or other help on following the instructions to mitigate the known vulnerabilities listed here, please subscribe to, and send your questions to the public Flume Users mailing list.

If you have encountered an unlisted security vulnerability or other unexpected behaviour that has security impact, or if the descriptions here are incomplete, please report them privately to the `Flume SecurityTeam <mailto:private@flume.apche.org>`__. Thank you!

.. rubric:: Fixed in Flume 1.11.0

`CVE-2022-42468 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-42468>`__: Apache Flume Improper Input Validation (JNDI Injection) in JMSSource.

+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+
| `CVE-2022-42468 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-42468>`__ | Deserialization of Untrusted Data                                        |
+====================================================================================+==========================================================================+
| Severity                                                                           | Moderate                                                                 |
+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+
| Base CVSS SCore                                                                    | 6.6 (AV:N/AC:H/PR:H/UI:N/S:U/C:H/I:H/A:H)                                |
+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+
| Versions Affected                                                                  | Flume 1.4.0 through 1.10.1                                               |
+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+

.. rubric:: Description

Apache Flume versions 1.4.0 through 1.10.1 are vulnerable to a remote code execution (RCE) attack when a configuration uses a JMS Source with an unsafe providerURL. This issue is fixed by limiting JNDI to allow only the use of the java protocol or no protocol.

.. rubric:: Mitigation

Do not use JMSSource or upgrade to Apache Flume 1.11.0

.. rubric:: Release Details

In release 1.11.0, if a protocol is specified in the providerUrl parameter only the java protocol will be allowed. If no protocol is specified it will also be allowed.

.. rubric:: Credit

This issue was found by Xian Wei.

.. rubric:: Fixed in Flume 1.10.1

`CVE-2022-34916 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-34916>`__: Apache Flume vulnerable to a JNDI RCE in JMSMessageConsumer.

+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+
| `CVE-2022-34916 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-34916>`__ | Deserialization of Untrusted Data                                        |
+====================================================================================+==========================================================================+
| Severity                                                                           | Moderate                                                                 |
+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+
| Base CVSS SCore                                                                    | 6.6 (AV:N/AC:H/PR:H/UI:N/S:U/C:H/I:H/A:H)                                |
+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+
| Versions Affected                                                                  | Flume 1.4.0 through 1.10.0                                               |
+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+

.. rubric:: Description

Flume's JMSMessageConsumer class can be configured with a destination name. A JNDI lookup is performed on this name without performing an validation. This could result in untrusted data being deserialized.

.. rubric:: Mitigation

Upgrade to Flume 1.10.1.

In releases 1.4.0 through 1.10.0 the JMSSource should not be used as it uses JMSMessageConsumer.

.. rubric:: Release Details

In release 1.10.1, if a protocol is specified in the destination name parameter only the java protocol will be allowed. If no protocol is specified it will also be allowed.

.. rubric:: Credit

This issue was found by Frentzen Amaral.


.. rubric:: Fixed in Flume 1.10.0

`CVE-2022-25167 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-25167>`__: Apache Flume vulnerable to a JNDI RCE in JMSSource.

+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+
| `CVE-2022-25167 <https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2022-25167>`__ | Deserialization of Untrusted Data                                        |
+====================================================================================+==========================================================================+
| Severity                                                                           | Moderate                                                                 |
+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+
| Base CVSS SCore                                                                    | 6.6 (AV:N/AC:H/PR:H/UI:N/S:U/C:H/I:H/A:H)                                |
+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+
| Versions Affected                                                                  | Flume 1.4.0 through 1.9.0                                                |
+------------------------------------------------------------------------------------+--------------------------------------------------------------------------+

.. rubric:: Description

Flume's JMSSource class can be configured with a connection factory name. A JNDI lookup is performed on this name without performing an validation. This could result in untrusted data being deserialized.

.. rubric:: Mitigation

Upgrade to Flume 1.10.0.

In releases 1.4.0 through 1.9.0 the JMSSource should not be used.

.. rubric:: Release Details

In release 1.10.0, if a protocol is specified in the connection factory parameter only the java protocol will be allowed. If no protocol is specified it will also be allowed.

.. rubric:: Credit

This issue was found by the Flume development team.
