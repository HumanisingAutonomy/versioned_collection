Getting started
================

Installation
------------

This document includes instructions to install MongoDB locally from scratch
and some of the settings that need to be updated to make it compatible with
``versioned_collection``, but any other method, such as using a Docker
container, will work.

Installing MongoDB
++++++++++++++++++
Firstly, make sure you install
`MongoDB <https://docs.mongodb.com/manual/installation/>`_, by choosing a
version greater or equal to ``5.0`` and enable the ``mongod`` process to start
at startup with the command

.. code-block:: sh

    sudo systemctl enable mongod.service


If you have any issues running ``mongod``, make sure that the permissions of the
directories/ files created by mongo are right.

.. code-block:: sh

    chown -R mongodb:mongodb /var/lib/mongodb
    chown -R mongodb:mongodb /var/log/mongodb

The next step is converting the standalone mongo instance to a
`replica set <https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/>`_
with a single replica instance (having more than one replica is also fine,
and it depends on the application). This step enables the
`Oplog <https://docs.mongodb.com/manual/core/replica-set-oplog/>`_, which allows
replication of the master database, but in this library it is only going to be
used to enable the use of
`Change Streams <https://docs.mongodb.com/manual/changeStreams/>`_, which are
used at the core of this library.

After enabling replica sets, add the name of the replica set to
the mongo config file. Edit the contents of ``/etc/mongod.conf`` to include the
following:

.. code-block:: none

    replication:
      replSetName: "rs0" # (or the name you chose for the replica set)

This will start the ``mongod`` service running with the correct configuration.

Installing the dependencies
+++++++++++++++++++++++++++

To solve the merge conflicts after pulling data from the remote collection,
this library uses the `Meld <https://meldmerge.org/>`_ mergetool. To install
Meld, run:

.. code-block:: sh

    sudo apt update
    sudo apt install -y meld

or manually download and install the package from the
`Meld <https://meldmerge.org/>`_ website.

Install ``versioned_collection``
++++++++++++++++++++++++++++++++++

.. code-block:: sh

    pip install versioned_collection



