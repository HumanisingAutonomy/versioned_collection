#! /bin/bash

if [ "$#" -eq 0 ]; then
    coverage run -m unittest discover tests && \
    mkdir .coverage_splits && \
    mv .coverage.*.*.* .coverage_splits && \
    touch .coverage && \
    coverage combine .coverage .coverage_splits/ && \
    rm -rf .coverage_splits && \
    coverage report
else
    python3 -m unittest $@
fi