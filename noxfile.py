from typing import Literal

import nox

nox.options.sessions = "tests"
main_version = ["3.10"]
supported_versions = main_version + []


def _lint(session: nox.Session, install_dependencies: bool = False) -> None:
    if install_dependencies:
        session.run("pip", "install", ".[lint]")
    session.run("flake8")


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
    session.run('coverage', 'run', '-m', 'unittest', 'discover', 'tests')
    session.run('mkdir', '.coverage_splits')
    session.run('bash', '-c', 'mv .coverage.*.*.* .coverage_splits')
    session.run('touch', '.coverage')
    session.run('coverage', 'combine', '.coverage', '.coverage_splits/')
    session.run('rm', '-rf', '.coverage_splits')
    session.run('coverage', 'report')
    session.run('coverage', report_format)


@nox.session(python=False)
def local_tests(session: nox.Session):
    _tests(session, report_format='html')


@nox.session(python=False)
def tests(session: nox.Session):
    _tests(session)


@nox.session(python=False)
def install(session: nox.Session):
    session.run("pip", "install", "-r", "docs/requirements.txt")
    session.run("pip", "install", "-r", "requirements.txt")


@nox.session(python=False)
def build(session: nox.Session):
    session.install(".[build]")
    session.run("python", "-m", "build", "--wheel")


@nox.session(python=False)
def docs(session: nox.Session, docs_format: str = 'html'):
    session.run("pip", "install", "-r", "docs/requirements.txt")
    session.run("bash", "-c", f"cd docs && make {docs_format}")
