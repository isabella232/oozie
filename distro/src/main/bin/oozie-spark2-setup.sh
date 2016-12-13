#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

OOZIE_SHARELIB_LOCATION="/opt/cloudera/parcels/CDH/lib/oozie/oozie-sharelib-yarn/lib/"
SPARK_SHARELIB_NAME="spark"
SPARK_SHARELIB_BACKUP_NAME="spark_orig"
SPARK2_JARS_FOLDER="/opt/cloudera/parcels/SPARK2/lib/spark2/jars"
SPARK2_PYSPARK_LIB_FOLDER="/opt/cloudera/parcels/SPARK2/lib/spark2/python/lib"

function printUsage() {
  echo
  echo " Usage  : oozie-spark2-setup.sh <Command and OPTIONS>"
  echo "          create                               creates a  sharelib for Spark2. The script uses the files under"
  echo "                                               /opt/cloudera/parcels/SPARK2/lib/spark2/jars and /opt/cloudera/parcels/SPARK2/lib/spark2/python/lib"
  echo "                                               and replaces the spark folder under /opt/cloudera/parcels/CDH/lib/oozie/oozie-sharelib-yarn/lib "
  echo "                                               while creating a backup with the name spark_orig"
  echo "          restore                              replaces the folder /opt/cloudera/parcels/CDH/lib/oozie/oozie-sharelib-yarn/lib/spark with"
  echo "                                               spark_orig if it exists."
  echo
}

function createSparkSharelibBackup() {
  if [ -d "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_BACKUP_NAME}" ]; then
    echo "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_BACKUP_NAME} exists, skip backup creation."
  else
    echo "Creating backup at ${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_BACKUP_NAME}"
    mv "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_NAME}" "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_BACKUP_NAME}"
  fi
}

function restoreSparkSharelibFromBackup() {
  if [ -d "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_BACKUP_NAME}" ]; then
    echo "Deleting ${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_NAME}"
    rm -rf "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_NAME}"
    echo "Moving ${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_BACKUP_NAME} to ${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_NAME}"
    mv "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_BACKUP_NAME}" "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_NAME}"
  else
    echo "No backup found at ${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_BACKUP_NAME}, exiting."
  fi
}

function linkFilesFrom() {
  for FILE in $@
  do
    LINK="${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_NAME}/$(basename "${FILE}")"
    echo "Linking ${FILE} to ${LINK}"
    ln -s "${FILE}" "${LINK}"
  done
}

function createSpark2Sharelib() {
  if [ -d "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_NAME}" ]; then
    rm -rf "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_NAME}"
  fi
  mkdir "${OOZIE_SHARELIB_LOCATION}${SPARK_SHARELIB_NAME}"
  linkFilesFrom "${SPARK2_JARS_FOLDER}"/*
  linkFilesFrom "${SPARK2_PYSPARK_LIB_FOLDER}"/*
  linkFilesFrom "/opt/cloudera/parcels/CDH/jars/oozie-sharelib-spark"*
}

set -e

if [ "$1" = "create" ]; then
  createSparkSharelibBackup
  createSpark2Sharelib
  exit 0
elif [ "$1" = "restore" ]; then
  restoreSparkSharelibFromBackup
  exit 0
else
  printUsage
  exit 1
fi

