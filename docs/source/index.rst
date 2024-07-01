.. VersionedCollection documentation master file, created by
   sphinx-quickstart on Mon May 23 20:31:23 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

:github_url: https://github.com/HumanisingAutonomy/versioned_collection

versioned_collection documentation
===============================================

VersionCollection is a library used to track and version MongoDB collections.

This library offers git-like functionalities for MongoDB collections, allowing
to register and checkout versions, to create branches and to synchronise
remote and local collections by pushing and pulling the changes between them.

.. toctree::
   :maxdepth: 1
   :caption: Python API

   Versioned Collection <versioned_collection.collection.versioned_collection>
   Tracking Collection <versioned_collection.collection.tracking_collections>
   Errors <versioned_collection.errors>
   Listener <versioned_collection.listener>

.. toctree::
   :maxdepth: 1
   :caption: Command Line

   command_line.rst

.. toctree::
   :glob:
   :maxdepth: 1
   :caption: Tutorials

   tutorials/introduction.rst
   tutorials/getting_started.rst
   tutorials/basics.rst

.. toctree::
   :glob:
   :maxdepth: 1
   :caption: Developer Notes

   notes/internals.rst
   notes/versioning_system.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`