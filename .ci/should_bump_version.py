#!/usr/bin/env python
from argparse import ArgumentParser


def get_args():
    parser = ArgumentParser()
    parser.add_argument("files", nargs='+', type=str)
    return parser.parse_args()


def main():
    to_ignore = (
        'VERSION', '.github/', '.ci/', 'versioned_collection/version.py'
    )
    should_bump = not all(
        [file.startswith(to_ignore) for file in get_args().files]
    )
    print(str(should_bump).lower())


if __name__ == '__main__':
    main()
