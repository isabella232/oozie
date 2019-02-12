#!/bin/bash -xe
# This is a temporary fix until we get the distributed test framework
# up and running in pre-commit
export JAVA8_BUILD=true
. /opt/toolchain/toolchain.sh
export PATH=$MAVEN_3_5_0_HOME/bin:$PATH
CDH_GBN=$(curl "http://builddb.infra.cloudera.com:8080/resolvealias?alias=${GIT_LOCAL_BRANCH}")
export CDH_GBN
env
curl "https://github.infra.cloudera.com/raw/CDH/cdh/${GIT_LOCAL_BRANCH}/gbn-m2-settings.xml" > mvn_settings.xml
JAVA8_BUILD=true
mvn clean test -s mvn_settings.xml -B -fae -Doozie.test.waitfor.ratio=2 -Dtest.timeout=10800 -Dmaven.test.java.opts="-Xmx2048m -da"
