# This is a temporary fix until we get the distributed test framework
# up and running in pre-commit

#!/bin/bash -xe
export JAVA8_BUILD=true
. /opt/toolchain/toolchain.sh
export branchName="cdh6.0.x"
export PATH=$MAVEN_3_5_0_HOME/bin:$PATH
export CDH_GBN=$(curl "http://builddb.infra.cloudera.com:8080/resolvealias?alias=${branchName}")
env
curl http://github.mtv.cloudera.com/raw/CDH/cdh/${branchName}/gbn-m2-settings.xml > mvn_settings.xml
JAVA8_BUILD=true
mvn clean test -s mvn_settings.xml -B -fae -Doozie.test.waitfor.ratio=2 -Dtest.timeout=10800 -Dmaven.test.java.opts="-Xmx2048m -da"
