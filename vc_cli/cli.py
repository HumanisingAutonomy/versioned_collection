#!/usr/bin/env python
""" A helper script to ease the interaction with a `VersionCollection`. """
import getpass
import itertools
import os.path
import pprint
import subprocess
import sys
import time
from argparse import ArgumentParser
from configparser import ConfigParser
from functools import partial
from os.path import expanduser
from threading import Thread, currentThread
from typing import Any, Tuple, Optional, Callable

from colorama import Fore
from pymongo import MongoClient

import versioned_collection
from versioned_collection import VersionedCollection
from versioned_collection.errors import BranchNotFound
from versioned_collection.utils.serialization import colour_diff

_CONFIG_DIR = f"{expanduser('~')}/.config/versioned_collection"
_CONFIG_FILE_PTH = os.path.join(_CONFIG_DIR, 'CONFIG')

_LOCAL_DB_SECTION = 'local'
_REMOTE_DB_SECTION = 'remote'


def _check_config_exits() -> None:
    if not os.path.exists(_CONFIG_DIR) or not os.path.exists(_CONFIG_FILE_PTH):
        _error("Error: Missing configuration.\n"
               "\tPlease run `vc config` before")
        exit(-1)


def _check_config_section_exists(
        config_file: ConfigParser,
        location: str = _LOCAL_DB_SECTION
) -> None:
    if not config_file.has_section(location):
        _loc_flag = '' if location == _LOCAL_DB_SECTION else '--remote'
        _error(f"Error: Missing {location} configuration.\n"
               f"\tPlease run `vc config` {_loc_flag} before")
        exit(-1)


def _get_config_file() -> ConfigParser:
    _check_config_exits()
    config_file = ConfigParser()
    config_file.read(_CONFIG_FILE_PTH)
    return config_file


def _check_db_col_set(config_file: ConfigParser,
                      location: str = _LOCAL_DB_SECTION
                      ) -> None:
    _check_config_section_exists(config_file, location)
    section = config_file[location]
    if 'database' not in section or 'collection' not in section:
        _loc_flag = '' if location == _LOCAL_DB_SECTION else '--remote'
        _error("Error: Missing database and collection details.\n"
               f"\tRun `vc use {_loc_flag}` before.")
        exit(-1)


def _get_current_database_and_collection(
        config_file: ConfigParser,
        location: str = _LOCAL_DB_SECTION
) -> Tuple[str, str]:
    _check_db_col_set(config_file)
    return config_file[location]['database'], \
        config_file[location]['collection']


def _error(msg: str) -> None:
    print(Fore.RED + msg)


def _info(msg: Any) -> None:
    print(Fore.GREEN + msg)


def _load_versioned_collection(
        config_file: Optional[ConfigParser] = None,
        location: str = _LOCAL_DB_SECTION
) -> VersionedCollection:
    if config_file is None:
        config_file = _get_config_file()

    _check_db_col_set(config_file, location)

    mongo_cfg = config_file[location]
    client = MongoClient(
        host=mongo_cfg['host'],
        port=int(mongo_cfg['port']),
        username=mongo_cfg.get('username', None),
        password=mongo_cfg.get('password', None)
    )

    vc = VersionedCollection(
        database=client[mongo_cfg['database']],
        name=mongo_cfg['collection'],
        username=mongo_cfg.get('username', None),
        password=mongo_cfg.get('password', None)
    )
    return vc


def _start_spinner(message: str) -> None:
    spinner = itertools.cycle(['-', '/', '|', '\\'])
    print(message, end=' ')
    t = currentThread()
    while getattr(t, "spin", True):
        sys.stdout.write(next(spinner))
        sys.stdout.flush()
        sys.stdout.write('\b')
        time.sleep(0.25)
    print(' ')


def _run_with_spinner(fn: Callable[..., Any], spinner_message: str) -> bool:
    spinner_thread = Thread(
        target=_start_spinner,
        args=(spinner_message,)
    )
    spinner_thread.spin = True
    spinner_thread.start()

    op_status = False
    try:
        op_status = fn()
    except Exception as e:
        _error(str(e))
        exit(-1)
    finally:
        spinner_thread.spin = False
        spinner_thread.join()
    return op_status


def use(args):
    config_file = _get_config_file()

    vc = _LOCAL_DB_SECTION if args.local else _REMOTE_DB_SECTION
    if (vc in config_file and
            'database' in config_file[vc]
            and config_file[vc]['database'] == args.database and
            'collection' in config_file[vc]
            and config_file[vc]['collection'] == args.collection):
        return
    else:
        if not config_file.has_section(vc):
            config_file[vc] = dict()
        config_file[vc]['database'] = args.database
        config_file[vc]['collection'] = args.collection

    with open(_CONFIG_FILE_PTH, 'w') as f:
        config_file.write(f)


def config(args):
    if not os.path.exists(_CONFIG_DIR):
        os.makedirs(_CONFIG_DIR)
    if not os.path.exists(_CONFIG_FILE_PTH):
        open(_CONFIG_FILE_PTH, 'w+').close()

    database_section = _LOCAL_DB_SECTION if args.local else _REMOTE_DB_SECTION

    config_file = ConfigParser()
    config_file.read(_CONFIG_FILE_PTH)
    if not config_file.has_section(database_section):
        config_file[database_section] = dict()
    mongo_cfg = config_file[database_section]

    if args.username is not None:
        mongo_cfg['username'] = args.username
        if args.password is None:
            _error("Error: Username provided without password. \n"
                   "\tInclude --password in your command.")
            exit(-1)
        else:
            if args.password == "prompt":
                password = getpass.getpass("Enter Password:")
            else:
                password = args.password
            mongo_cfg['password'] = password

    if not args.local and args.host is None:
        _error("Error: Tried to configure the remote database, but no host "
               "address was provided.\n\tInclude --host in your command if "
               "--remote is set.")
        exit(-1)

    mongo_cfg['host'] = 'localhost' if args.host is None else args.host
    mongo_cfg['port'] = str(args.port)

    with open(_CONFIG_FILE_PTH, 'w') as f:
        config_file.write(f)


def config_show(_) -> None:
    _get_config_file().write(sys.stdout)


def _print_remote_status(local: VersionedCollection,
                         remote: VersionedCollection
                         ) -> None:
    if local == remote:
        print(Fore.YELLOW + "Local and remote have all branches synchronised")
    elif not (local <= remote or remote <= local):
        print(Fore.YELLOW +
              "Local and remote have divergences. The local collection has "
              "versions that the remote one does not have and the remote "
              "collection has versions the local collection does not have.")
    else:
        # One is behind the other. Show the status just fo the current local
        # branch
        try:
            remote_log = remote.get_log(local.branch)
        except BranchNotFound:
            print(Fore.YELLOW +
                  f"The local branch {local.branch} does not exist on remote")
            return
        local_log = local.get_log()

        separation_point = None
        for local, remote in zip(reversed(local_log), reversed(remote_log)):
            if not local.weakly_equals(remote):
                break
            separation_point = remote.version, remote.branch
        else:
            # No divergence found
            dif = len(local_log) - len(remote_log)
            if dif > 0:
                msg = f"Local branch '{local.branch}' is ahead of remote by " \
                      f"{dif} versions"
            elif dif < 0:
                msg = f"Local branch '{local.branch}' is behind remote by " \
                      f"{abs(dif)} versions"
            else:
                msg = f"Local branch '{local.branch}' is up to date " \
                      f"with the remote branch"

            print(Fore.YELLOW + msg)
            return

        print(Fore.YELLOW + f"Local branch {local.branch} has diverged from "
                            f"the remote branch after version "
                            f"'{separation_point}'.")


def status(_) -> None:
    cfg = _get_config_file()
    collection = _load_versioned_collection(cfg)
    db, col = _get_current_database_and_collection(cfg)

    print(Fore.WHITE + f"Using database: {db} collection: {col}")

    if not collection.is_tracked():
        _error("Collection not initialised for versioning")
        return

    if cfg.has_section(_REMOTE_DB_SECTION):
        try:
            remote_collection = _load_versioned_collection(
                cfg, _REMOTE_DB_SECTION
            )
            _print_remote_status(collection, remote_collection)
        except Exception:
            _error("Not able to get status data for the remote collection. "
                   "Make sure that `vc` is properly configured and the remote "
                   "collection is accessible")

    _status = collection.status()
    changed = _status['changed']
    collection_status = pprint.pformat(_status, sort_dicts=False)

    print()
    colour = Fore.RED if changed else Fore.GREEN
    print(colour + collection_status)


def log(args):
    if args.tree:
        print(Fore.RED + "--tree option not supported yet")
    collection = _load_versioned_collection()

    try:
        pager = subprocess.Popen(['less', '-F', '-R', '-X', '-K'],
                                 stdin=subprocess.PIPE, stdout=sys.stdout)
        for l in collection.get_log(args.branch):
            pager.stdin.write(f"{Fore.GREEN + str(l)} \n".encode())
        pager.stdin.close()
        pager.wait()
    except KeyboardInterrupt:
        # '-K' flag of less handles this case
        pass
    except Exception as e:
        _error(str(e))
        exit(-1)


def diff(args):
    diffs = None
    try:
        diffs = _load_versioned_collection().diff(
            args.version, args.branch, deep=False
        )
    except Exception as e:
        _error(str(e))
        exit(-1)

    if diffs is None:
        _error("Collection untracked. Cannot compute diffs")
    if not len(diffs):
        _info("Nothing has changed since last version registered.")

    try:
        pager = subprocess.Popen(['less', '-F', '-R', '-X', '-K'],
                                 stdin=subprocess.PIPE, stdout=sys.stdout)
        for doc_id, diff_str in diffs.items():
            diff_str = colour_diff(diff_str)
            pager.stdin.write(('\n' + Fore.YELLOW + f"Document {doc_id}\n"
                               + diff_str + '\n').encode())
        pager.stdin.close()
        pager.wait()
    except KeyboardInterrupt:
        # the '-K' flag of less handles this case
        pass
    except BrokenPipeError:
        # This is caused when we normally exit the pager view, but we still
        # try to write to stdin, so nothing to be done here, as well
        pass


def init(args):
    collection = _load_versioned_collection()
    try:
        collection.init(message=args.message)
    except Exception as e:
        _error(str(e))
        exit(-1)


def create_branch(args):
    collection = _load_versioned_collection()
    try:
        collection.create_branch(args.branch_name)
    except Exception as e:
        _error(str(e))
        exit(-1)


def branches(_):
    _branches = _load_versioned_collection().branches()
    if len(_branches) == 0:
        _info("Collection has no branches")
    else:
        _info(str(_branches))


def register(args):
    collection = _load_versioned_collection()
    fn = partial(collection.register,
                 message=args.message,
                 branch_name=args.branch
                 )
    _status = _run_with_spinner(fn, spinner_message='Registering a new version')
    if _status:
        _info("Successfully registered a new version.")
    else:
        _info("Status clear. Nothing to register.")


def discard_changes(_):
    config_file = _get_config_file()
    db, col = _get_current_database_and_collection(config_file)
    msg = f"Are you sure you want to discard the changes for <{db}>:<{col}>?" \
          f" (y/n): "
    answer = input(msg).lower()
    if answer == 'n':
        return

    collection = _load_versioned_collection(config_file)
    _run_with_spinner(
        collection.discard_changes,
        spinner_message='Discarding changes'
    )
    _info("Changes successfully discarded")


def checkout(args):
    collection = _load_versioned_collection()
    fn = partial(collection.checkout,
                 version=args.version,
                 branch=args.branch
                 )
    _run_with_spinner(fn, spinner_message='Checking out')

    msg = "You are now"
    if collection.version != -1:
        msg += f" at version {collection.version}"
    msg += f" on branch {collection.branch}."
    _info(msg)


def stash(args):
    collection = _load_versioned_collection()
    _status = False
    try:
        _status = collection.stash(args.overwrite)
    except Exception as e:
        _error(str(e))
        exit(-1)

    if _status:
        _info("Changes stashed")
    else:
        _info(f"Nothing to stash. Collection {collection.name} is clear")


def stash_apply(_):
    _status = False
    try:
        _status = _load_versioned_collection().stash_apply()
    except Exception as e:
        _error(str(e))
        exit(-1)

    if _status:
        _info("Stash applied")
    else:
        _info(f"Nothing to stash. Stash area is clear")


def stash_discard(_):
    _load_versioned_collection().stash_discard()
    _info("Stash cleared")


def push(args):
    local = _load_versioned_collection()
    remote = _load_versioned_collection(location=_REMOTE_DB_SECTION)

    fn = partial(local.push,
                 remote_collection=remote,
                 branch=args.branch,
                 do_checkout=args.do_checkout
                 )

    _status = _run_with_spinner(fn, spinner_message='Pushing to remote')

    if _status:
        _info("Branch pushed successfully")
    else:
        _error("Local collection not initialised for versioning")


def pull(args):
    local = _load_versioned_collection()
    remote = _load_versioned_collection(location=_REMOTE_DB_SECTION)

    fn = partial(local.pull,
                 remote_collection=remote,
                 branch=args.branch,
                 )

    _status = _run_with_spinner(fn, spinner_message='Pulling from remote')

    if _status:
        _info("Branch pulled successfully")
    else:
        _error("Remote not initialised for versioning")


def resolve_conflicts(args):
    collection = _load_versioned_collection()
    if not collection.has_conflicts():
        _info("No conflicts to resolve")
        return
    try:
        collection.resolve_conflicts(args.discard_local_changes)
    except KeyboardInterrupt:
        print(Fore.YELLOW + "Conflict resolution interrupted.")
        if collection.has_conflicts():
            _error("Collection still has unresolved conflicts")
        exit(0)
    except Exception as e:
        _error(str(e))
        exit(-1)

    if not collection.has_conflicts():
        _info("All conflicts have been resolved")


def delete_version(args):
    collection = _load_versioned_collection()

    fn = partial(collection.delete_version_subtree,
                 version=args.version,
                 branch=args.branch
                 )
    _status = _run_with_spinner(fn, spinner_message='Deleting versions')

    if _status:
        _info("Versions successfully deleted")

        if not collection.is_tracked():
            _info("The collection is now empty")
            return

        if collection.version != -1:
            version_format = f"at version {collection.version}"
        else:
            version_format = ""
        msg = "You are currently {} on branch {}".format(
            version_format, collection.branch)
        print(Fore.YELLOW + msg)
    else:
        _error("Collection untracked")


def listen(_):
    collection = _load_versioned_collection()
    modified = 0
    if not collection.has_changes():
        modified = collection._modified_collection.count_documents({})  # noqa

    def wait():
        while True:
            time.sleep(0.25)

    try:
        _run_with_spinner(
            wait,
            spinner_message='Listening... Press <Ctrl + c> to stop'
        )
    except KeyboardInterrupt:
        _info("Listener successfully stopped")

        pass

    new_modified = collection._modified_collection.count_documents({})  # noqa
    if new_modified - modified > 0:
        collection._has_changed()  # noqa


def cli():
    parser = ArgumentParser(prog='vc')

    parser.add_argument(
        '-v', '--version', action='version',
        version=f'versioned_collection: {versioned_collection.__version__}',
        help='Show the current versioned_collection version installed'
    )

    subparsers = parser.add_subparsers(
        title='These are common VersionedCollection commands',
        metavar='commands'
    )

    # config
    config_parser = subparsers.add_parser(
        'config',
        help='Update the configuration and credentials'
    )
    _config_group = config_parser.add_mutually_exclusive_group(required=False)
    _config_group.add_argument(
        '--local', dest='local', default=True, action='store_true',
        help='whether to set the configuration for the local database'
    )
    _config_group.add_argument(
        '--remote', dest='local', action='store_false',
        help='whether to set the configuration for the remote database'
    )
    config_parser.add_argument(
        '--username', type=str, default=None,
        help='user with access to the database'
    )
    config_parser.add_argument(
        '--password', nargs='?', const='prompt',
        help='password to access the database. '
             'if unfilled, a prompt will appear.'
    )
    config_parser.add_argument(
        '--host', type=str, default=None,
        help='host address of the mongodb server'
    )
    config_parser.add_argument(
        '--port', type=int, default=27017,
        help='port of the mongodb server'
    )
    config_parser.set_defaults(handle=config)

    config_subparser = config_parser.add_subparsers(
        title='The available subcommands',
        metavar='commands'
    )

    # ##  config show
    config_show_parser = config_subparser.add_parser(
        'show',
        help='Print the contents of the current configuration',
    )
    config_show_parser.set_defaults(handle=config_show)

    # use
    use_parser = subparsers.add_parser(
        'use',
        help='Set the database and the collection to use'
    )
    _use_group = use_parser.add_mutually_exclusive_group(required=False)
    _use_group.add_argument(
        '--local', dest='local', default=True, action='store_true',
        help='whether to update the collection and database names for the '
             'local collection'
    )
    _use_group.add_argument(
        '--remote', dest='local', action='store_false',
        help='whether to update the collection and database names for the '
             'remote collection'
    )
    use_parser.add_argument(
        '-d', '--database', type=str, required=True,
        help='database containing the versioned collection'
    )
    use_parser.add_argument(
        '-c', '--collection', type=str, required=True,
        help='name of the versioned collection'
    )
    use_parser.set_defaults(handle=use)

    # status
    status_parser = subparsers.add_parser(
        'status',
        help='Show the status of the version tree'
    )
    status_parser.set_defaults(handle=status)

    # init
    init_parser = subparsers.add_parser(
        'init',
        help='Initialise a collection for versioning'
    )
    init_parser.add_argument(
        '-m', '--message', type=str, default=None,
        help='initialise a collection for versioning'
    )
    init_parser.set_defaults(handle=init)

    # create_branch
    branch_parser = subparsers.add_parser(
        'create_branch',
        help='Create a new branch pointing at the current version'
    )
    branch_parser.add_argument(
        'branch_name', type=str,
        help='name of the new branch'
    )
    branch_parser.set_defaults(handle=create_branch)

    # register
    register_parser = subparsers.add_parser(
        'register',
        help='Register a new version of the collection'
    )
    register_parser.add_argument(
        '-m', '--message', type=str, required=True,
        help='message that describes the changes in this version'
    )
    register_parser.add_argument(
        '-b', '--branch', type=str, default=None,
        help='branch where the version should be registered'
    )
    register_parser.set_defaults(handle=register)

    # checkout
    checkout_parser = subparsers.add_parser(
        'checkout',
        help='Check out a tracked version of the collection'
    )
    checkout_parser.add_argument(
        'version', type=int, default=None,
        help='the version to check out'
    )
    checkout_parser.add_argument(
        '-b', '--branch', type=str, default=None,
        help='the branch of the version to check out'
    )
    checkout_parser.set_defaults(handle=checkout)

    # log
    log_parser = subparsers.add_parser(
        'log',
        help='Show version logs'
    )
    log_parser.add_argument(
        '-b', '--branch', type=str, default=None,
        help='the branch for which to get the log'
    )
    log_parser.add_argument('--tree', action='store_true')
    log_parser.set_defaults(handle=log)

    # branches
    get_branches_parser = subparsers.add_parser(
        'branches',
        help='Show the existing branches of the collection'
    )
    get_branches_parser.set_defaults(handle=branches)

    # diff
    diff_parser = subparsers.add_parser(
        'diff',
        help='Compute the diff between the current version and another version'
    )
    diff_parser.add_argument(
        '-v', '--version', type=int, default=None,
        help='the version against which to compute the diff'
    )
    diff_parser.add_argument(
        '-b', '--branch', type=str, default=None,
        help='the branch of the version against which to compute the diff'
    )
    diff_parser.set_defaults(handle=diff)

    # discard changes
    reset_parser = subparsers.add_parser(
        'discard_changes',
        help='Discard the unregistered changes of the collection'
    )
    reset_parser.set_defaults(handle=discard_changes)

    # stash
    stash_parser = subparsers.add_parser(
        'stash',
        help='Stash the changes of the collection. See subcommand for help'
    )
    stash_parser.add_argument(
        '--overwrite', type=bool, default=False,
        help='overwrite the current stash area'
    )
    stash_parser.set_defaults(handle=stash)

    stash_subparsers = stash_parser.add_subparsers(
        title='The available subcommands',
        metavar='commands'
    )

    # ##  stash apply
    stash_apply_parser = stash_subparsers.add_parser(
        'apply',
        help='Apply the stashed changes back to the collection',
    )
    stash_apply_parser.set_defaults(handle=stash_apply)

    # ##  stash discard
    stash_discard_parser = stash_subparsers.add_parser(
        'discard',
        help='Clear the stash area',
    )
    stash_discard_parser.set_defaults(handle=stash_discard)

    # delete version
    delete_version_parser = subparsers.add_parser(
        'delete_version',
        help='Delete a version and all the successor versions of it'
    )
    delete_version_parser.add_argument(
        '-v', '--version', type=int, default=None,
        help='the version to delete'
    )
    delete_version_parser.add_argument(
        '-b', '--branch', type=str, default=None,
        help='the branch of the version to delete'
    )
    delete_version_parser.set_defaults(handle=delete_version)

    # push
    push_parser = subparsers.add_parser(
        'push',
        help='Update remote collection by uploading a branch to it'
    )
    push_parser.add_argument(
        '-b', '--branch', type=str, default=None,
        help='the branch to push'
    )
    push_parser.add_argument(
        '--do_checkout', type=bool, default=True,
        help='checks out the latest pushed version if remote is on the pushed '
             'branch'
    )
    push_parser.set_defaults(handle=push)

    # pull
    pull_parser = subparsers.add_parser(
        'pull',
        help='Fetch from and integrate a branch from a remote collection'
    )
    pull_parser.add_argument(
        '-b', '--branch', type=str, default=None,
        help='the branch to pull'
    )
    pull_parser.set_defaults(handle=pull)

    # resolve conflicts
    conflicts_parser = subparsers.add_parser(
        'resolve_conflicts',
        help='Resolve the merge conflicts'
    )
    conflicts_parser.add_argument(
        '--discard_local_changes', type=bool, default=False,
        help='ignore the local changes and keep the remote documents only'
    )
    conflicts_parser.set_defaults(handle=resolve_conflicts)

    # listen
    listen_parser = subparsers.add_parser(
        'listen',
        help='Start monitoring the changes made to the collection.'
    )
    listen_parser.set_defaults(handle=listen)

    args = parser.parse_args()

    if hasattr(args, 'handle'):
        args.handle(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    cli()
