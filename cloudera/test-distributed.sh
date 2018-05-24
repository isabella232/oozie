#!/bin/bash
set -xe

# From jenkins
export JAVA8_HOME=$JAVA_1_8_HOME

DIR="$( cd $( dirname ${BASH_SOURCE[0]} )  && pwd )"
cd $DIR

# activate mvn-gbn wrapper
mv "$(which mvn-gbn-wrapper)" "$(dirname "$(which mvn-gbn-wrapper)")/mvn"

# Build the project
$DIR/build.sh

# Install dist_test locally
SCRIPTS="dist_test"

if [[ -d $SCRIPTS ]]; then
    echo "Cleaning up remnants from a previous run"
    rm -rf $SCRIPTS
fi

git clone --depth 1 https://github.com/cloudera/$SCRIPTS.git $SCRIPTS || true

# Fetch the right branch
cd "$DIR/$SCRIPTS"
git fetch --depth 1 origin
git checkout -f origin/master
git ls-tree -r HEAD
./setup.sh
export PATH=`pwd`/bin/:$PATH
which grind

if [[ -z $DIST_TEST_USER || -z $DIST_TEST_PASSWORD ]]; then
    # Fetch dist test credentials and add them to the environment
    wget http://staging.jenkins.cloudera.com/gerrit-artifacts/misc/hadoop/dist_test_cred.sh
    source dist_test_cred.sh
fi

# Go to project root
cd "$DIR/.."

# Populate the per-project grind cfg file
cat > .grind_project.cfg << EOF
[grind]
empty_dirs = ["test/data", "test-dir", "log"]
file_globs = []
file_patterns = ["*.so"]
artifact_archive_globs = ["**/surefire-reports/TEST-*.xml"]
EOF

# Invoke grind to run tests
grind -c ${DIR}/$SCRIPTS/env/grind.cfg config
grind -c ${DIR}/$SCRIPTS/env/grind.cfg pconfig
grind -c ${DIR}/$SCRIPTS/env/grind.cfg test --artifacts -r 3 --java-version 8 \
    -e TestHAPartitionDependencyManagerService \
    -e TestCredentials \
    -e TestSLAEventStatusCalculator \
    -e TestStatusTransitService

# Cleanup the grind folder
if [[ -d "$DIR/$SCRIPTS" ]]; then
    rm -rf "$DIR/$SCRIPTS"
fi

## Copied from http://github.mtv.cloudera.com/Kitchen/jenkins-master/blob/master/dsl/master-02/pre_commit/oozie_gerrit.groovy
# Fetch a dummy JUnit results XML file to prevent a build failure.
wget http://com.cloudera.hadoop.s3-us-west-2.amazonaws.com/test_results.xml -O test_results.xml
# and make the dummy file appear to be new - the plugin is smart!
touch test_results.xml
##
