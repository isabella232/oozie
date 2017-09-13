#!/bin/bash
set -xe

DIR="$( cd $( dirname ${BASH_SOURCE[0]} )  && pwd )"
# we run mr1 tests only after promotion
./bin/mkdistro.sh -Pmr1 -DskipTests
# run tests with mr2
./bin/mkdistro.sh
