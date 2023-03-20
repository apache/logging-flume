==========================================
Apache Flume Spring Boot Source Repository
==========================================

.. rubric:: Overview

This project uses `Git <http://git-scm.com/>`_ to manage its source code. Instructions on
Git use can be found in the `Git documentation <http://git-scm.com/documentation/>`_.

.. rubric:: Web Access

The following is a link to the online source repository.

.. raw:: html

   <div class="highlight-none"><div class="highlight"><pre>
   <a href="https://github.com/apache/flume-spring-boot">https://github.com/apache/flume-spring-boot</a>
   </pre></div></div>

.. rubric:: Anonymous Access

The source can be checked out anonymously from git with this command:::

    $ git clone https://github.com/apache/flume-spring-boot.git

.. rubric:: Developer Access

Everyone can access the Git repository via HTTP, but Committers must clone the git repository via HTTPS.::

    $ git clone git@github.com:apache/flume-spring-boot.git

Committers should first commit the patch to trunk on your local repo::

    $ git pull
    $ git commit -m "A message"
    $ git log

Copy the commit hash of the commit you just made. Then::

    $ git pull
    $ git cherry-pick <commit hash of the commit you made> (This should get committed immediately).

Now push to trunk::

    $ git push -u origin trunk

For more details, please read: `Git at Apache <https://gitbox.apache.org/>`_.

Please note that the main development branch is **trunk**, not master. 

.. rubric:: Access Through a Proxy

The Git client can go through a proxy, if you configure it to do so. To configure git to use a proxy::

    $ git config http.proxy address:port

Then do a git clone as usual::

    $ git clone https://github.com/apache/flume-spring-boot

**Note 2:** Committers could use either Apache Gitbox or Github repositories. SVN can no longer be used to commit.
