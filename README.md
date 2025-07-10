![](media/logo.png)

----

[![Tests](https://github.com/HumanisingAutonomy/versioned_collection/actions/workflows/release.yaml/badge.svg)](https://github.com/HumanisingAutonomy/versioned_collection/actions/workflows/release.yaml)
[![codecov](https://codecov.io/github/HumanisingAutonomy/versioned_collection/graph/badge.svg?token=5AS1HJSQAW)](https://codecov.io/github/HumanisingAutonomy/versioned_collection)
[![Documentation](https://readthedocs.org/projects/versioned-collection/badge/?version=latest)](https://versioned-collection.readthedocs.io/latest/)

Enable collection versioning in MongoDB with `VersionedCollection`. A
`VersionedCollection` can be used like a normal `pymongo`
[Collection](https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html),
but it also supports git-like functionality, such as registering versions,
branching and synchronising two collections of the same type by pulling and 
pushing data between them.

## Installation

<details>
  <summary>MongoDB</summary>


Firstly, make sure you install
[MongoDB>=5.0](https://docs.mongodb.com/manual/installation/), and enable 
[replica sets](https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/)
with a single replica instance.
</details>

<details>
  <summary>Versioned Collection</summary>


```bash
pip install versioned_collection
```
</details>

<details>
  <summary>Dependencies</summary>  


  To solve the merge conflicts after pulling data from the remote collection, 
  this library uses the [Meld](https://meldmerge.org/) mergetool. To install 
  Meld, run:
  
  ```bash
  sudo apt update
  sudo apt install -y meld
  ```
  or manually download and install the package from the 
  [Meld](https://meldmerge.org/) website.

</details>


## Basic example

<details>
  <summary>Expand</summary>  


To enable versioning on a collection, firstly create a class that inherits
from `VersionedCollection`. All the interactions with the collection should be
done through this class, and not by using the database directly with `mongosh`
or other database management or querying programs, and also not by directly
accessing the collection using the `pymongo` driver.

```python
import pymongo
from versioned_collection import VersionedCollection


client = pymongo.MongoClient("mongodb://localhost:27017")
db = client['database_name']
bands_collection = VersionedCollection(db, name='bands')

bands_collection.insert_one({'name': 'Led Zeppelin'})
bands_collection.init('Initial collection version')

bands_collection.insert_one({'name': 'Black Sabbath'})
bands_collection.register(message='Second version')
```

### Note

<details>
  <summary>Access control</summary>  
If access control is enabled, the username and the password of a user that has
`readWrite` permissions to the database where the target collection is located
should be provided:

```python
host, port, user, password = get_params()

client = pymongo.MongoClient(
    host=host,
    port=port,
    username=user,
    password=password,
)

db = client['database_name']
bands_collection = VersionedCollection(
    db, 'bands', 
    username=user, 
    password=password
)
```

Alternatively, `username` and `password` can be updated based on the 
environment variables `VC_MONGO_USER` and `VC_MONGO_PASSWORD` respectively. The
code provided variables have priority.
</details>

</details>

## Common operations


| versioned_collection     | git                                  | remarks                                                                                                                                                                                  |
|--------------------------|--------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `register`               | `commit`                             | Registering a _version_ of a collection is equivalent to committing the changes                                                                                                          |
| `checkout`               | `checkout`                           |                                                                                                                                                                                          |
| `create_branch`          | `branch`                             | Create a new branch. Branches in  `versioned_collection`  are just pointers to a registered version, as branches in  git are just pointers to commits.                                   |
| `stash`                  | `stash`                              | Stashes the changes.                                                                                                                                                                     |
| `stash apply`            | `stash apply`                        | Applies the stashed changes. The  `versioned_collection`  differs from the  git one, and overwrites the new state of the collection with the stashed changes (does not perform a merge). |
| `stash discard`          | `stash drop`                         | Clears the stashed changes.                                                                                                                                                              |
| `delete_version_subtree` | `reset --hard <hash>`                | Removes a version and all the subsequent registered versions.                                                                                                                            |
| `discard_changes`        | `git reset --hard && git clean -fxd` | Removes all of the unregistered changes.                                                                                                                                                 |
| `diff`                   | `diff`                               | Computes the  _diffs_ between the current version and another version.                                                                                                                   |
| `log`                    | `log`                                | Inspect the version log similarly as the commit log can be viewed.                                                                                                                       |
| `pull`                   | `pull`                               | Pulls the changes from a remote collection to the local collection.                                                                                                                      |
| `push`                   | `push`                               | Pushes the changes from the local collection to a remote collection.                                                                           |


## Using the CLI

<details>
<summary>Expand</summary>  


After installing this library, refresh the shell or open a new one. A CLI should have been installed and can be accessed by typing `vc`.

```
usage: vc [-h] command ...

optional arguments:
  -h, --help       show this help message and exit

These are common VersionedCollection commands:
  commands
    config                Update the configuration and credentials
    use                   Set the database and the collection to use
    status                Show the status of the version tree
    init                  Initialise a collection for versioning
    create_branch         Create a new branch pointing at the current version
    register              Register a new version of the collection
    checkout              Check out a tracked version of the collection
    log                   Show version logs
    branches              Show the existing branches of the collection
    diff                  Compute the diff between the current version and another version
    discard_changes       Discard the unregistered changes of the collection
    stash                 Stash the changes of the collection. See subcommand for help
    delete_version        Delete a version and all the successor versions of it
    push                  Update remote collection by uploading a branch to it
    pull                  Fetch from and integrate a branch from a remote collection
    resolve_conflicts     Resolve the merge conflicts
    listen                Start monitoring the changes made to the collection.

```

Firstly, make sure you run `vc config` to configure the connection details
to the mongo server. See `vc config -h` for the list of parameters.

```
usage: vc config [-h] [--local | --remote] [--username USERNAME] [--password [PASSWORD]] [--host HOST] [--port PORT] commands ...

optional arguments:
  -h, --help              show this help message and exit
  --local                 whether to set the configuration for the local database
  --remote                whether to set the configuration for the remote database
  --username USERNAME     user with access to the database
  --password [PASSWORD]   password to access the database. if unfilled, a prompt will appear.
  --host HOST             host address of the mongodb server
  --port PORT             port of the mongodb server

The available subcommands:
  commands
    show                  Print the contents of the current configurationon

```

Use the flags `--local` and `--remote` to update the database connection
information for the local or the remote collection. If no flag is passed,
by default the configuration for the local database is updated. Note that
the 'remote' can be on the same host, but in other database.

To perform versioning operations on a versioned collection make sure you
select it by using `vc use`.

```
usage: vc use [-h] -d DATABASE -c COLLECTION

optional arguments:
  -h, --help            show this help message and exit
  -d DATABASE, --database DATABASE
                        Database containing the versioned collection
  -c COLLECTION, --collection COLLECTION
                        Name of the versioned collection
```

</details>

## Building the documentation locally

<details>
  <summary>Expand</summary>  


To build documentation in various formats, you will need
[Sphinx](https://www.sphinx-doc.org/en/master/):

```bash
nox -s docs
```

This will build the documentation in html format. If other formats are
preferred, run

```bash
nox -s "docs(docs_format='<format>')"
```
</details>



## Additional resources

For more comprehensive examples check the following resources:

- [Python API](https://versioned-collection.readthedocs.io/latest/versioned_collection.collection.versioned_collection.html)
- [Basic usage tutorial](https://versioned-collection.readthedocs.io/latest/tutorials/basics.html)
- [Notes on CLI](https://versioned-collection.readthedocs.io/latest/command_line.html)
- [Implementation details](https://versioned-collection.readthedocs.io/latest/notes/internals.html) for a more in-depth
  description of the internal workings of this library
- [Advanced notes on the versioning system](https://versioned-collection.readthedocs.io/latest/notes/versioning_system.html)







