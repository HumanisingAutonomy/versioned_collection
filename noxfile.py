from pathlib import Path
import shutil
from typing import Literal

import toml

import nox

nox.options.sessions = [
    "lint",
    "build",
    "install",
    "tests",
    "docs(docs_format='html')",
]


@nox.session(python=False)
def install_core(session: nox.Session) -> None:
    session.run("pip", "install", ".[lint,tests,build]")


@nox.session(python=False)
def install(session: nox.Session) -> None:
    session.run("pip", "install", ".[lint,tests,build,documentation]")


def _lint(session: nox.Session, install_dependencies: bool = False) -> None:
    if install_dependencies:
        session.run("pip", "install", ".[lint]")
    session.run("flake8")
    session.run("pyink", "--check", ".")


@nox.session(python=False)
def local_lint(session: nox.Session) -> None:
    _lint(session)


@nox.session(python=False)
def lint(session: nox.Session) -> None:
    _lint(session)


def _tests(
    session: nox.Session,
    install_dependencies: bool = False,
    report_format: Literal['html', 'xml'] = 'xml',
) -> None:
    if install_dependencies:
        session.install(".[tests]")
    session.run('./scripts/run_tests.sh')
    session.run('coverage', report_format)


@nox.session(python=False)
def local_tests(session: nox.Session):
    _tests(session, report_format='html')


@nox.session(python=False)
def tests(session: nox.Session):
    _tests(session)


@nox.session(python=False)
def build(session: nox.Session):
    version = None
    file_name = "pyproject.toml"
    saved_file_name = f"{file_name}.orig"

    if "--version" in session.posargs:
        version = session.posargs[session.posargs.index("--version") + 1]
        shutil.copyfile(file_name, saved_file_name)

        file = Path(file_name)
        data = toml.load(file)
        data["project"]["version"] = version
        data["project"]["dynamic"].remove("version")
        file.write_text(toml.dumps(data))

    session.run("python", "-m", "build")

    if version is not None:
        shutil.move(saved_file_name, file_name)


@nox.session(python=False)
@nox.parametrize('docs_format', ['html'])
def docs(session: nox.Session, docs_format: str = "html") -> None:
    if "--version" in session.posargs:
        version = session.posargs[session.posargs.index("--version") + 1]
    else:
        from setuptools_git_versioning import get_version

        version = str(get_version())
    session.run(
        "bash", "-c", f"cd docs && make {docs_format}", env={"VERSION": version}
    )
