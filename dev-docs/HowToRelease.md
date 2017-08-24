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
Apache Flume: How to Release
=============================

This document describes how to make a release of Flume. It is a work in
progress and should be refined by the [Release
Manager](http://incubator.apache.org/guides/releasemanagement.html#glossary-release-manager)
(RM) as they come across aspects of the release process not yet
documented here.

NOTE: For the purpose of illustration, this document assumes that the
version being released is 1.0.0, and the following development version
will become 1.1.0.

Prerequisites
-------------

### Policy documents

The policy on releasing artifacts from an incubating Apache project is
stated in the [Guide to Release Management During
Incubation](http://incubator.apache.org/guides/releasemanagement.html#glossary-release-manager).
While Flume is now a top-level project, that guide is still a good
resource for new release managers since it contains what are considered
to be best practices.

The Release Manager (RM) must go through the policy document to
understand all the tasks and responsibilities of running a release.

### Give a heads up

The RM should first create an umbrella issue and then setup a timeline
for release branch point. The time for the day the umbrella issue is
created to the release branch point must be at least two weeks in order
to give the community a chance to prioritize and commit any last minute
features and issues they would like to see in the upcoming release.

The RM should then send the pointer to the umbrella issue along with the
tentative timeline for branch point to the user and developer lists. Any
work identified as release related that needs to be completed should be
added as a subtask of the umbrella issue to allow users to see the
overall release progress in one place.

Send out an e-mail like this:

    To: dev@flume.apache.org
    Subject: [DISCUSS] Flume 1.7 release plan

    We have almost 100 commits since 1.6.0, and a bunch of new features
    and improvements including a Taildir Source, many upgrades,
    performance increases, plus a lot of fixes and documentation
    additions.

    It seems to me that it's time to cut a 1.7 release soon. I would be
    happy to volunteer to RM.

    Below is the list of tickets that have patches submitted and are
    scheduled for 1.7.0:
    https://issues.apache.org/jira/browse/FLUME-2998?jql=project%20%3D%20FLUME%20AND%20status%20%3D%20%22Patch%20Available%22%20AND%20resolution%20%3D%20Unresolved%20AND%20fixVersion%20%3D%20v1.7.0 (18 tickets)

    There are more tickets that have no patch submitted:
    https://issues.apache.org/jira/browse/FLUME-2998?jql=project%20%3D%20FLUME%20AND%20issuetype%20in%20standardIssueTypes()%20AND%20resolution%20%3D%20Unresolved%20AND%20fixVersion%20%3D%20v1.7.0 (30 tickets)

    I suggest tracking the release process using the JIRA at https://issues.apache.org/jira/browse/FLUME-2924

    If this all sounds OK, I'd like to suggest targeting the week of
    October 10 for a first RC. (Also, anything that is non-trivial should
    get in by October 7. Any feature or big change that is not committed
    by then would be scheduled for the next release.)

    That should leave enough time for people to get moving on patches and
    reviews for their documentation improvements, critical bug fixes, and
    low-risk enhancements, etc.

    Also, I would like to propose October 7 (one week from today) for the
    branch date.

    Please let me know your thoughts.


### JIRA cleanup

1\. Before a release is done, make sure that any issues that are fixed
have their fixVersion setup correctly. Run the following JIRA query to
see which resolved issues do not have their fix version set up
correctly:

**`JIRA: project = Flume and Resolution = Fixed and fixVersion is EMPTY`**

The result of the above query should be empty. If some issues do show up
in this query that have been fixed since the last release, please
bulk-edit them to set the fix version to the version being released.

2\. Since the JIRA release note tool will list all JIRAs associated with
a particular fixVersion regardless of their Resolution, you may want to
remove fixVersion on issues declared as "Duplicate", "Won't Fix", "Works
As Expected", etc. You can use this query to find those issues, then
just bulk edit them and leave the fixVersion field blank when editing
them. (Note that you will need to replace "v1.4.0" below with the
version you are trying to release.)

**`JIRA: project = Flume AND (Status != Resolved OR Resolution != Fixed) AND fixVersion = v1.6.0`**

3\. Finally, check out the output of the **[JIRA release note
tool](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311321)**
to see which JIRAs are included in the release, in order to do a sanity
check.

### Monitor active issues

It is important that between the time that the umbrella issue is filed
to the time when the release branch is created, no experimental or
potentially destabilizing work is checked into the trunk. While it is
acceptable to introduce major changes, they must be thoroughly reviewed
and have good test coverage to ensure that the release branch does not
start of being unstable.

If necessary the RM can discuss if certain issues should be fixed on the
trunk in this time, and if so what is the gating criteria for accepting
them.

Creating Release Artifacts
--------------------------

### Communicate with the community

1\. Send an email to dev@flume.apache.org to

Notify that you are about to branch.

Ask to hold off any commits until this is finished.

2\. Send another email after branching is done.

### Update the LICENSE file

The release manager is responsible for updating the LICENSE file to
provide accurate license information for all binary artifacts contained
in the codebase. This is a tedious and painstaking process, and must be
performed for each release that includes a binary artifact.

### Prepare branches and create tag

In this section, the release is X.Y.Z (e.g. 1.3.0)

1\. Checkout the trunk.

    git clone http://git-wip-us.apache.org/repos/asf/flume.git flume

2\. Update CHANGELOG in the trunk to indicate the changes going into the
new version.

The change list can be swiped from the **[JIRA release note
tool](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311321)**
(choose the "text" format for the change log). See JIRA Cleanup above to
ensure that the release notes generated by this tool are what you are
expecting. Additionally, in the last two releases we have re-ordered the
issues to be New Features, Improvements, Bugs, etc as by default the
least important items are at the top.

3\. Update the "version" value in RELEASE-NOTES in trunk to X.(Y+1).0:

    vim RELEASE-NOTES
    git add RELEASE-NOTES
    git commit RELEASE-NOTES -m "Updating Flume version in RELEASE-NOTES for release X.Y.Z"

4\. Create a branch for the X.(Y+1) release series

E.g. flume-1.5 should be branched off of trunk

    git checkout trunk
    git checkout -b flume-X.(Y+1)

4\. Update the "version" value of all pom.xml and documentation files in
trunk to X.(Y+1).0-SNAPSHOT

    git checkout trunk
    find . -name pom.xml | xargs sed -i "" -e "s/X.Y.0-SNAPSHOT/X.$(Y+1).0-SNAPSHOT/"
    find flume-ng-doc/ -name "*.rst" | xargs sed -i "" -e "s/X.Y.0-SNAPSHOT/X.$(Y+1).0-SNAPSHOT/"
    git add .
    git commit -m "Updating trunk version to X.$(Y+1).0-SNAPSHOT for Flume X.Y.Z release"

5\. Checkout the release branch and remove -SNAPSHOT from the release
branch poms and docs and commit:

    git checkout flume-X.Y

    find . -name pom.xml | xargs sed -i "" -e "s/X.Y.0-SNAPSHOT/X.Y.0/"
    find flume-ng-doc/ -name "*.rst" | xargs  sed -i "" -e "s/X.Y.0-SNAPSHOT/X.Y.0/"
    git add .
    git commit -m "FLUME-XXXX: Removing -SNAPSHOT from X.Y branch"

6\. Ensure RELEASE-NOTES has the appropriate version and description of
the release.

7\. Push the branching changes upstream


    git push -u origin trunk:trunk
    git push -u origin flume-X.Y:flume-X.Y


8\. Tag a release candidate (in the example below, RC1):


    git tag -a release-X.Y.Z-rc1 -m "Apache Flume X.Y.Z RC1"
    git push origin release-X.Y.Z-rc1


If an rc2, rc3 etc is needed, simply create a new rc tag:


    git tag -d release-X.Y.Z-rc2
    git push origin release-X.Y.Z-rc2


### Preparing to sign the artifacts

All artifacts must be signed and checksummed. In order to sign a release
you will need a PGP key. You should get your key signed by a few other
people. You will also need to recv their keys from a public key server.
See the [Apache release signing](https://www.apache.org/dev/release-signing)
page for more details.

1\. Add your key to the
[KEYS](https://dist.apache.org/repos/dist/release/flume/KEYS) file:


    (gpg --list-sigs <your-email> && gpg --armor --export <your-email>) >> KEYS


And commit the changes.


### Generating and signing the source artifacts

There is a script in the Flume source tree for generating and signing the Flume
source artifacts. Once the release candidate is tagged, generate the source
release with the following steps.

1\. From the top of the Flume source tree, create a directory for the artifacts
    and then generate them:


    mkdir ./source-artifacts
    ./dev-support/generate-source-release.sh X.Y.Z release-X.Y.Z-rc1 ./source-artifacts/


The artifacts will be placed in the directory you specify (in this case,
`./source-artifacts`)


### Testing the source tarball

1\. Unpack the source tarball


    tar xzvf apache-flume-X.Y.Z-src.tar.gz


2\. Do a full build inside the source tarball. Allow all unit tests &
integration tests to run and also include the docs.


    cd apache-flume-X.Y.Z-src
    export LC_ALL=C.UTF-8 # Required to build the javadocs on some platforms and in some locales
    mvn clean install -Psite -DskipTests


3\. Verify that the HTML docs that should have been generated inside the
binary artifact under /docs are there and do not have rendering errors.


### Generating, signing, and deploying the binary artifacts

Maven is configured to generate, sign, and deploy the binary artifacts
automatically. Use the following steps to do that:


1\. Create and sign the artifacts, including site docs. This pushes the
signed artifacts to the ASF staging repository.

In order to do this, you will need a settings.xml file with your
username and password for the ASF staging repository. Typically this is
placed in \~/.m2/settings.xml and might look something like this:


    <settings>
      <servers>
        <server>
          <id>apache.staging.https</id>
          <username>your_user_id</username>
          <password>your_password</password>
        </server>
      </servers>
    </settings>


2\. Once your settings.xml file is correct, run the following from the
Flume source directory to generate and deploy the artifacts:


    mvn clean deploy -Psite -Psign -DskipTests


This will sign, checksum, and upload each artifact to Nexus.

Note: the checksum files will not be mirrored; They should be downloaded
from the main apache dist site.

Do the same for javadoc and source artifacts:

    mvn javadoc:jar gpg:sign deploy:deploy
    mvn source:jar gpg:sign deploy:deploy


3\. Publish Staging repository

Login to <https://repository.apache.org> and select Staging Repositories
on the left under Build Promotion.

Select org.apache.flume from the list of repositories, verify it looks
OK, and then click Close using "Apache Flume X.Y.Z" as the description
to allow others to see the repository. Note that the staging repository
will have a numeric id associated with it that will be used later

4\. Copy the source artifacts you built locally to people.apache.org

    $ rsync -e ssh -av source-artifacts/apache-flume-X.Y.Z-src.tar.gz* people.apache.org:public_html/apache-flume-X-Y.Z-rcN/

5\. Copy the binary artifacts you deployed via Maven to people.apache.org

    $ ssh people.apache.org
    $ cd public_html
    $ mkdir apache-flume-X.Y.Z-rcN
    $ cd apache-flume-X.Y.Z-rcN
    $ wget --no-check-certificate https://repository.apache.org/content/repositories/orgapacheflume-XXXX/org/apache/flume/flume-ng-dist/X.Y.Z/flume-ng-dist-X.Y.Z-bin.tar.gz{,.{asc,md5,sha1}}
    $ for file in flume-ng-dist-*; do mv $file $(echo $file | sed -e "s/flume-ng-dist/apache-flume/g");done


Running the vote
----------------

### Call for dev list votes

Send an email to dev@flume.apache.org list. For example,

    To: dev@flume.apache.org
    Subject: [VOTE] Release Apache Flume version X.Y.Z RC1

    This is the XXXXX release for Apache Flume as a top-level project,
    version X.Y.Z. We are voting on release candidate RC1.

    It fixes the following issues:
      <Link-to-CHANGELOG-in-the-tag>

    *** Please cast your vote within the next 72 hours ***

    The tarball (*.tar.gz), signature (*.asc), and checksums (*.md5, *.sha1)
    for the source and binary artifacts can be found here:
      https://people.apache.org/~mpercy/flume/apache-flume-X.Y.Z-RC1/

    Maven staging repo:
      https://repository.apache.org/content/repositories/orgapacheflume-XXXXX/

    The tag to be voted on:
      https://git-wip-us.apache.org/repos/asf?p=flume.git;a=commit;h=<commit-hash-of-the-tag>

    Flume's KEYS file containing PGP keys we use to sign the release:
      https://svn.apache.org/repos/asf/flume/dist/KEYS


You need 3 +1 votes from Flume PMC members for a release.

When the vote gets cancelled or passes, send out an e-mail like:

    This vote is cancelled.

    I'm about to send an e-mail about RC2.

or (if the vote passes):

    This vote is now closed.

    I will send out the results in a separate email.

    Thanks to all who voted!


### Send results of a successful vote

After a successful RC vote has passed, send an email to dev@flume.apache.org list. For example,

    To: dev@flume.apache.org
    Subject: [RESULT] Flume 1.7.0 release vote

    The release vote for Apache Flume 1.7.0 has been completed in this
    thread: https://lists.apache.org/thread.html/3cf114125dbea6e662b69d1312c34184690af327900e8dcccea5edde@%3Cdev.flume.apache.org%3E

    The vote has received 3 binding +1 votes from the following PMC members:
    Mike Percy
    Hari Shreedharan
    Brock Noland

    Four non-binding +1 votes were received from:
    Denes Arvay
    Bessenyei Balázs Donát
    Lior Zeno
    Attila Simon

    No +0 or -1 votes were received.

    Since three +1 votes were received from the PMC with no -1 votes, the
    vote passes and Flume 1.7.0 RC2 will be promoted to the Flume 1.7.0
    release.

    Thanks to all who voted!


Rolling out the Release
-----------------------

### Upload the artifacts

#### Source and convenience artifacts

    svn checkout https://dist.apache.org/repos/dist/release/flume dist-flume
    cd dist-flume
    mkdir 1.0.0
    cp <all release artifacts including asc/checksum files> 1.0.0/
    svn rm stable # remove older version link
    ln -s 1.0.0 stable # update stable version link
    svn add stable 1.0.0
    svn commit


It may take up to 24 hours for all mirrors to sync up.

#### Deploy Maven artifacts

General instructions on how to deploy the poms and jars to Maven Central
can be found at the [Apache page on publishing maven
artifacts](https://www.apache.org/dev/publishing-maven-artifacts.html).

**Flume-specific instructions:**

After you get your `settings.xml` file configured correctly (see above
link) then you can run:
`mvn clean deploy -Psite -DskipTests -Papache-release` to push the
artifacts to the staging repo.\
You will need to go to the Apache Maven Repository @
<http://repository.apache.org/> and log in with your Apache LDAP
credentials\
Once you log in you will see Build Promotion &gt; Staging Repositories
on the left hand side\
You will want to edit the Flume artifacts that you don't want to push to
Maven (you can delete stuff like the release tarball)\
Click Close to make the atrifacts available on the Staging repository.\
To push to Central, you click Release.

### Announce the release

Send an email to announce@apache.org (the from: address must be
@apache.org). For example,


    To: announce@apache.org, user@flume.apache.org, dev@flume.apache.org
    Subject: [ANNOUNCE] Apache Flume 1.2.0 released

    The Apache Flume team is pleased to announce the release of Flume
    version 1.2.0.

    Flume is a distributed, reliable, and available service for efficiently
    collecting, aggregating, and moving large amounts of log data.

    This release can be downloaded from the Flume download page at:
    http://flume.apache.org/download.html

    The change log and documentation are available on the 1.2.0 release page:
    http://flume.apache.org/releases/1.2.0.html

    Your help and feedback is more than welcome. For more information on how
    to report problems and to get involved, visit the project website at
    http://flume.apache.org/

    The Apache Flume Team

### Update the website

1.  Checkout <https://svn.apache.org/repos/asf/flume/site/trunk>
2.  Add a page to the `content/sphinx/releases` directory for the new
    release, with the Changelog and links to the documentation (refer
    previous release pages for details. The documentation should simply
    use the same paths as the previous releases with correct versions -
    the documentation will be checked in directly to the production
    website as mentioned below).
3.  Update `content/sphinx/releases/index.rst` to update the pointer to
    the latest release.
4.  Update `content/sphinx/download.rst` to also point to the
    latest release.
5.  Update `content/sphinx/index.rst` as necessary to add a News item to
    the home page.
6.  Copy the release version of the FlumeUserGuide.rst and
    FlumeDeveloperGuide.rst to the Documentation directory.
7.  Commit the changes to svn. Go to <https://cms.apache.org/flume/> -
    Stage and publish the changes.
8.  Checkout
    <https://svn.apache.org/repos/infra/websites/production/flume>.
9.  Create a directory with the current release's directory name under
    content/releases/content (e.g., content/releases/content/1.3.1)
10. Copy both the HTML and PDF versions of the user guide and developer
    guide (manually generate the PDF from the HTML files), and the
    javadocs (apidocs directory) into this directory.
11. Commit the changes to svn. Done!
