#!/usr/bin/bash

format=html
if [ $# -eq 1 ]
  then
    format=$1
fi

cd docs
make $format