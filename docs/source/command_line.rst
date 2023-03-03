.. _command-line-interface:

Command Line Interface
========================

After installing the ``versioned_collection`` library, the main versioning
commands are available via a command line interface.

We will not cover the semantics of the available commands, here, so please
refer to :ref:`this page <basic-concepts>` for that.

The available commands can be listed by running:

.. code-block:: shell

    $ vc -h
.. code-block:: none

    usage: vc [-h] commands ...

    optional arguments:
      -h, --help         show this help message and exit

    These are common VersionedCollection commands:
      commands
        config              Update the configuration and credentials
        use                 Set the database and the collection to use
        status              Show the status of the version tree
        init                Initialise a collection for versioning
        create_branch       Create a new branch pointing at the current version
        register            Register a new version of the collection
        checkout            Check out a tracked version of the collection
        log                 Show version logs
        branches            Show the existing branches of the collection
        diff                Compute the diff between the current version and another version
        discard_changes     Discard the unregistered changes of the collection
        stash               Stash the changes of the collection. See subcommand for help
        delete_version      Delete a version and all the successor versions of it
        push                Update remote collection by uploading a branch to it
        pull                Fetch from and integrate a branch from a remote collection
        resolve_conflicts   Resolve the merge conflicts
        listen              Start monitoring the changes made to the collection.

.. warning::
    It is important to remember that there are two valid ways of interacting
    with a collection such that the changes are tracked:

        *  use the Python client and use the ``VersionedCollection`` class
        *  call ``vc listen`` in a terminal tab, modify the collection and
           then make sure to stop listening (``Ctrl + c``) before registering
           a version or running any other ``vc`` commands.


Configuring the local and remote databases
--------------------------------------------

Use ``vc config`` to configure the connection strings to the local and remote
database:

.. code-block::

    $ vc config -h
    usage: vc config [-h] [--local | --remote] [--username USERNAME] [--password [PASSWORD]] [--host HOST] [--port PORT] commands ...

    optional arguments:
      -h, --help            show this help message and exit
      --local               whether to set the configuration for the local database
      --remote              whether to set the configuration for the remote database
      --username USERNAME   user with access to the database
      --password [PASSWORD] password to access the database. if unfilled, a prompt will appear.
      --host HOST           host address of the mongodb server
      --port PORT           port of the mongodb server

    The available subcommands:
      commands
        show

For example, to configure the local database, run:

.. code-block::

    $ vc config --local --username myuser --host localhost --port 27017 --password supersecret

Then, calling ``vc config show`` to display the config yields the following:

.. code-block::

    $ vc config show

    [local]
    host = localhost
    port = 27017
    username = myuser
    password = supersecret

Using a collection
--------------------------------------------
To specify to which collection ``versioned_collection`` should connect, we
can use the ``vc use`` command. We can then switch between collections and
databases, to specify on which collections the ``vc`` commands act. This is
similar to changing the current working database in ``mongosh`` or ``sql``.

.. code-block::

    $ vc use -h
    usage: vc use [-h] [--local | --remote] -d DATABASE -c COLLECTION

    optional arguments:
      -h, --help            show this help message and exit
      --local               whether to update the collection and database names for the local collection
      --remote              whether to update the collection and database names for the remote collection
      -d DATABASE, --database DATABASE
                            database containing the versioned collection
      -c COLLECTION, --collection COLLECTION
                            name of the versioned collection

Again, we have the ``--local`` and ``--remote`` flags to specify whether we
want to modify the local or the remote collection.

.. code-block::

    $ vc use --local -c my_collection -d my_database
    $ vc config show

    [local]
    host = localhost
    port = 27017
    database = my_database
    collection = my_collection
    username = myuser
    password = supersecret




