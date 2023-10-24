#!/usr/bin/env python
import datetime
import os

from setuptools import setup


def fetch_version() -> str:
    import pathlib
    from configparser import ConfigParser
    # root directory of the project
    version_file = pathlib.Path(__file__).parent.resolve()
    version_file = version_file.joinpath('VERSION')
    config_file = ConfigParser()
    config_file.read(version_file)
    return config_file['version']['number']


def main():
    version = fetch_version()

    if 'VC_DEV_BUILD' in os.environ:
        version = (
            f"{version}+nightly{datetime.datetime.now().strftime('%d%m%y')}"
        )

    setup(version=version)


if __name__ == "__main__":
    main()
