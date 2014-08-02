#!/usr/bin/env bash

# (c) Copyright 2012 WibiData, Inc.
#
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ------------------------------------------------------------------------------

# The enable-profiling script performs the actions required to use the
# profiling jars for KijiSchema and KijiMR, instead of standard jars.
# The corresponding disable-profiling script performs clean up so that
# you can use the standard (non-profiling) jars again.
#
# Run the command as follows before running any kiji commands
#
# $KIJI_HOME/bin/profiling/enable-profiling.sh
# kiji <command> etc...
# $KIJI_HOME/bin/profiling/disable-profiling.sh

# ------------------------------------------------------------------------------

set -o nounset   # Fail when referencing undefined variables
set -o errexit   # Script exits on the first error
set -o pipefail  # Pipeline status failure if any command fails
if [[ ! -z "${DEBUG:-}" ]]; then
  source=$(basename "${BASH_SOURCE}")
  PS4="# ${source}":'${LINENO}: '
  set -x
fi

# ------------------------------------------------------------------------------

if [[ -z "${KIJI_HOME}" ]]; then
  echo "Please set the KIJI_HOME enviroment variable before you enable profiling."
  exit 1
fi

if [[ ! -f "${KIJI_HOME}/conf/kiji-schema.version" ]]; then
  error "Invalid KIJI_HOME=${KIJI_HOME}"
  error "Cannot find \${KIJI_HOME}/conf/kiji-schema.version"
  exit 1
fi
kiji_schema_version=$(cat "${KIJI_HOME}/conf/kiji-schema.version")

if [[ ! -f "${KIJI_HOME}/conf/kiji-mapreduce.version" ]]; then
  error "Invalid KIJI_HOME=${KIJI_HOME}"
  error "Cannot find \${KIJI_HOME}/conf/kiji-mapreduce.version"
  exit 1
fi
kiji_mr_version=$(cat "${KIJI_HOME}/conf/kiji-mapreduce.version")

if [[ -z "${HADOOP_HOME}" ]]; then
  echo "Please set the HADOOP_HOME environment variable before you enable profiling."
  exit 1
fi

# Name of the profiled version of the KijiSchema jar
profiling_kiji_schema_jar_name="kiji-schema-profiling-${kiji_schema_version}.jar"
# Name of the original KijiSchema jar
kiji_schema_jar_name="kiji-schema-${kiji_schema_version}.jar"

# Name of the profiled version of the KijiSchema jar
profiling_kiji_mr_jar_name="kiji-mapreduce-profiling-${kiji_mr_version}.jar"
# Name of the original KijiSchema jar
kiji_mr_jar_name="kiji-mapreduce-${kiji_mr_version}.jar"

# Create a directory to move the non-profiling jars to, so we can install
# profiling-enabled jars in their normal place
orig_dir="${KIJI_HOME}/lib/original_jars"

# Name of the aspectj jar
aspectj_jar_name="aspectjrt-1.7.2.jar"

# Create a directory for original jars
mkdir -p "${orig_dir}"

# Flag to indicate some unexpected things happened along the way. We may be
# in an inconsistent state
inconsistent_state="false"

# Move KijiSchema jar out of the way
if [[ -f "${KIJI_HOME}/lib/${kiji_schema_jar_name}" ]]; then
  echo "Moving the non-profiled KijiSchema jar from "\
  "${KIJI_HOME}/lib/${kiji_schema_jar_name} to ${orig_dir}"
  mv "${KIJI_HOME}/lib/${kiji_schema_jar_name}" "${orig_dir}"
else
  echo "${KIJI_HOME}/lib/${kiji_schema_jar_name} does not exist. Is profiling enabled?"
  inconsistent_state="true"
fi

# Copy profiling KijiSchema jar into the $KIJI_HOME/lib directory
if [[ ! -f "${KIJI_HOME}/lib/${profiling_kiji_schema_jar_name}" ]]; then
  echo "Moving the profiling enabled KijiSchema jar from " \
  "${KIJI_HOME}/lib/profiling/${profiling_kiji_schema_jar_name} to ${KIJI_HOME}/lib"
  cp "${KIJI_HOME}/lib/profiling/${profiling_kiji_schema_jar_name}" "${KIJI_HOME}/lib"
else
  echo "Profiling enabled jar already exists in ${KIJI_HOME}/lib. Not overwriting."
  inconsistent_state="true"
fi

# Copy the aspectj jar into the $KIJI_HOME/lib directory
if [[ ! -f "${KIJI_HOME}/lib/${aspectj_jar_name}" ]]; then
  echo "Moving the aspectj jar from " \
  "${KIJI_HOME}/lib/profiling/${aspectj_jar_name} to ${KIJI_HOME}/lib"
  cp "${KIJI_HOME}/lib/profiling/${aspectj_jar_name}" "${KIJI_HOME}/lib"
else
  echo "Aspectj jar already exists in ${KIJI_HOME}/lib. Not overwriting."
  inconsistent_state="true"
fi

# Move Kiji MapReduce jar out of the way
if [[ -f "${KIJI_HOME}/lib/${kiji_mr_jar_name}" ]]; then
  echo "Moving the non-profiled Kiji MapReduce jar from "\
  "${KIJI_HOME}/lib/${kiji_mr_jar_name} to ${orig_dir}"
  mv "${KIJI_HOME}/lib/${kiji_mr_jar_name}" "${orig_dir}"
else
  echo "${KIJI_HOME}/lib/${kiji_mr_jar_name} does not exist. Is profiling enabled?"
  inconsistent_state="true"
fi

# Copy profiling Kiji MapReduce jar into the $KIJI_HOME/lib directory
if [[ ! -f "${KIJI_HOME}/lib/${profiling_kiji_mr_jar_name}" ]]; then
  echo "Moving the profiling enabled Kiji MapReduce jar from " \
  "${KIJI_HOME}/lib/profiling/${profiling_kiji_mr_jar_name} to ${KIJI_HOME}/lib"
  cp "${KIJI_HOME}/lib/profiling/${profiling_kiji_mr_jar_name}" "${KIJI_HOME}/lib"
else
  echo "Profiling enabled jar already exists in ${KIJI_HOME}/lib. Not overwriting."
  inconsistent_state="true"
fi

if ! "${inconsistent_state}"; then
  echo ""
  echo "Profiling has been enabled."
  echo ""
else
  echo "Please check the error messages. " \
    "Some manual actions may be required to enable profiling."
  exit 1
fi
