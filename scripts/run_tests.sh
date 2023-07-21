#! /bin/bash

if [ "$#" -eq 0 ]; then
    # Not sure what's the best way of handling this. The tests using
    # the in-memory database cannot be run together with the ones using
    # the actual mongodb instance since something somehow breaks, but I don't
    # understand exactly why.
    # Until finding a better solution, tests should be grouped in modules and
    # invoked as such
    coverage run -m unittest discover tests/misc && \
    coverage run -m unittest discover tests/test_versioned_collection && \
    coverage run -m unittest discover tests/test_tracking_collection && \
    mkdir .coverage_splits && \
    mv .coverage.*.*.* .coverage_splits && \
    touch .coverage && \
    coverage combine .coverage .coverage_splits/ && \
    rm -rf .coverage_splits && \
    coverage report
else
    python3 -m unittest $@
fi