# This is a temporary fix until we get the distributed test framework
# up and running in pre-commit

#!/bin/bash
set -xe
# From jenkins
export JAVA8_HOME=$JAVA_1_8_HOME


# activate mvn-gbn wrapper
mv "$(which mvn-gbn-wrapper)" "$(dirname "$(which mvn-gbn-wrapper)")/mvn"

mvn clean install -B -fae
