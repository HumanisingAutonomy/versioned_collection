Introduction to versioned\_collection
=====================================

.. currentmodule:: versioned_collection


``versioned_collection`` is Python library that allows tracking and versioning
MongoDB collections. The data required for versioning is stored in the same
database as the collection to be versioned, this approach having the
advantage of keeping all the data needed for versioning in a single place,
allowing for instance for easier and more intuitive backups or migrations of
versioned collections.

There are two main ways of interacting with a versioned collection:

  *  using the Python API
  *  starting to listen to the collection via the command line client and
     then updating the documents in any other way, e.g., via
     `mongosh <https://www.mongodb.com/docs/mongodb-shell/>`_,
     `Compass <https://www.mongodb.com/products/compass>`_, etc.

What is a VersionedCollection
-----------------------------
A :class:`VersionedCollection` extends the pymongo
`Collection <https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection>`_
class by adding support for versioning in a way similar to ``git``. A
:class:`VersionedCollection` can be used in the same way a pymongo collection
is used without any overhead in the speed of the execution of the
MongoDB operations. However, this introduces one of the tradeoffs of this
library, making some of the versioning operations more expensive to run
in terms of execution time.

Basic operations and concepts
------------------------------

Knowing the basics of ``git``, learning to use ``versioned_collection`` becomes
trivial since the operations allowed on a versioned collection are a subset of
the operations allowed in ``git``, most of them having a similar semantics.
Here is a table of the ``versioned_collection`` operations and concepts
and their ``git`` correspondent:

.. list-table:: versioned_collection and git operations correspondence
    :widths: 10 15 50
    :header-rows: 1

    * - versioned_collection
      - git
      - remarks

    * - ``register``
      - ``commit``
      -  Registering a `version` of a collection is equivalent to committing
         the changes

    * - ``checkout``
      - ``checkout``
      -

    * - ``create_branch``
      - ``branch``
      -  Create a new branch. Branches in ``versioned_collection`` are just
         pointers to a registered version, as branches in ``git`` are just
         pointers to commits.

    * - ``stash``
      - ``stash``
      - Stashes the changes.

    * - ``stash apply``
      - ``stash apply``
      - Applies the stashed changes. The ``versioned_collection`` differs from
        the ``git`` one, and overwrites the new state of the collection with the
        stashed changes (does not perform a merge).

    * - ``stash discard``
      - ``stash drop``
      - Clears the stashed changes.

    * - ``delete_version_subtree``
      - ``reset --hard <hash>``
      - Removes a version and all the subsequent registered versions.

    * - ``discard_changes``
      - ``git reset --hard && git clean -fxd``
      - Removes all the unregistered changes.

    * - ``diff``
      - ``diff``
      -  Computes the `diffs` between the currently checkout version and
         another version.

    * - ``log``
      - ``log``
      - Inspect the version log similarly as the commit log can be viewed.

    * - ``pull``
      - ``pull``
      - Pulls the changes from a remote collection to the local collection.

    * - ``push``
      - ``push``
      - Pushes the changes from the local collection to a remote collection.

.. warning::
    The syntax of the commands and the available options differs from git,
    but the meaning of the concepts is similar.

.. note::
    Versioned collection can be seen as ``git`` repositories, but the notion
    of remote and local collections (local and remote repositories) is weaker.

For a list of all allowed operations check the
:ref:`Python API documentation <versioned-collection-api>` and the
:ref:`command line client examples <command-line-interface>`.