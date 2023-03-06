__version__ = '0.0.2'

from typing import Optional


def _fetch_version() -> Optional[str]:
    import pathlib
    from configparser import ConfigParser
    # root directory of the project
    version_file = pathlib.Path(__file__).parent.parent.resolve()
    version_file = version_file.joinpath('VERSION')
    if not version_file.exists():
        return None
    config_file = ConfigParser()
    config_file.read(version_file)
    return config_file['version']['number']


def _update_version_if_needed(config_version: str) -> str:
    if __version__ != config_version:
        with open(__file__, 'r+') as f:
            rest_of_lines = f.read().splitlines()[1:]
            f.seek(0)
            new_version_line = f"__version__ = '{config_version}'"
            f.write('\n'.join([new_version_line] + rest_of_lines))
    return config_version


# It is useful to have the version embedded into this package as
# well, because after the package has been installed, the access to the
# `VERSION` configuration can be lost. If available, the configuration is the
# main source of truth, otherwise we rely on this file to provide the correct
# version number.
if (__config_version := _fetch_version()) is not None:
    __version__ = _update_version_if_needed(__config_version)
