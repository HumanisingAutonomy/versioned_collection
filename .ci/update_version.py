import pathlib
from argparse import ArgumentParser
from configparser import ConfigParser


def get_args():
    parser = ArgumentParser()
    parser.add_argument("version", type=str)
    return parser.parse_args()


def main():
    args = get_args()
    project_root = pathlib.Path(__file__).parent.parent.resolve()
    version_file = project_root.joinpath('VERSION')
    config_file = ConfigParser()
    config_file.read(version_file)
    config_file['version']['number'] = args.version

    # save the warning and update
    with open(version_file, 'r+') as f:
        comment = f.readlines()[-3:]
        f.seek(0)
        config_file.write(f)
        f.writelines(comment)

    # Update the package's version.py
    version_file = project_root.joinpath('versioned_collection', 'version.py')
    new_version_line = f"__version__ = '{args.version}'"
    with open(version_file, 'r+') as f:
        rest_of_lines = f.read().splitlines()[1:]
        f.seek(0)
        f.write('\n'.join([new_version_line] + rest_of_lines))


if __name__ == '__main__':
    main()
