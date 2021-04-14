=================
Source Repository
=================

.. rubric:: Overview

This project uses `Git <http://git-scm.com/>`_ to manage its source code. Instructions on
Git use can be found in the `Git documentation <http://git-scm.com/documentation/>`_.

.. rubric:: Web Access

The following is a link to the online source repository.

.. raw:: html

   <div class="highlight-none"><div class="highlight"><pre>
   <a href="https://github.com/apache/flume">https://github.com/apache/flume</a>
   </pre></div></div>

.. rubric:: Anonymous Access

Flume maintains an active release branch along with trunk. The release branch represents the list of commits that will go into the next release.
All commits that go into trunk will also have to be committed using git cherry-pick and pushed to the current release branch.
The current release branch is flume-1.9.

The source can be checked out anonymously from git with this command:::

    $ git clone https://github.com/apache/flume.git

.. rubric:: Developer Access

Everyone can access the Git repository via HTTP, but Committers must clone the git repository via HTTPS.::

    $ git clone git@github.com:apache/flume.git

Committers should first commit the patch to trunk on your local repo::

    $ git pull
    $ git commit -m "A message"
    $ git log

Copy the commit hash of the commit you just made. Then::

    $ git checkout flume-1.9 (or the current release branch)
    $ git pull
    $ git cherry-pick <commit hash of the commit you made> (This should get committed immediately).

Now push to both trunk and the release branch::

    $ git push -u origin trunk
    $ git push -u origin flume-1.9

For more details, please read: `Git at Apache <https://gitbox.apache.org/>`_.

The committer should make sure the commits are pushed to both branches. Please make sure all commits to the release branch are fast forward commits
and there are no merge commits on the release branch.

This process requires a little more work, but this guarantees that our release tags will not have accidental and local commits in its history,
as we can force push to the release branches to remove these from history. Ideally we should try to keep the history on release branches linear,
but if at some point we decide to start using feature branches, we might end up having merge commits on these branches too, but that is expected
and required - since that would represent the list of commits for that feature.

When a release is finalized, the current release branch will be frozen by the release manager for the release, and a new release branch will be
branched off the current release branch. The new branch will represent the next release and all commits not meant for the current release must
go to the new branch once the current branch is frozen. The release manager then pushes release related commits to the current branch.

For example, if the next release is flume-1.9.0, all commits should go to trunk and flume-1.9.
When the rolling release canidate, the release manager will create a new branch, say flume-1.10 from the latest commit of 
flume-1.9. All further non-release related commits should go to trunk and flume-1.10 (unless the release manager thinks otherwise - in which case it can go to flume-1.9 and flume-1.10). 

Please note that the main development branch is **trunk**, not master. 

.. rubric:: Access Through a Proxy

The Subversion client can go through a proxy, if you configure it to do so. To configure git to use a proxy::

    $ git config http.proxy address:port

Then do a git clone as usual::

    $ git clone https://github.com/apache/flume


.. rubric:: SVN Access

If you prefer to use svn and not git, please use the `Github mirror <https://github.com/apache/flume>`_ to checkout
the sources. Please read `this <https://github.com/blog/626-announcing-svn-support>`_ and `this <https://github.com/blog/626-announcing-svn-support>`_
for details on how to use svn with github.

**Note 1:** The (erstwhile) Apache Flume `SVN repository <http://svn.apache.org/repos/asf/flume/>`_ is no longer updated and should not be used to checkout
the source code. 

**Note 2:** Committers could use either Apache Gitbox or Github repositories. SVN can no longer be used to commit.
