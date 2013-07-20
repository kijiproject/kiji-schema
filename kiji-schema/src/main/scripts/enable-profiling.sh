#!/usr/bin/env bash
#
#   (c) Copyright 2013 WibiData, Inc.
#
#   See the NOTICE file distributed with this work for additional
#   information regarding copyright ownership.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#   The enable-profiling script performs the actions required to use the
#   profiling jars for KijiSchema and KijiMR, instead of standard jars.
#   The corresponding disable-profiling script performs clean up so that
#   you can use the standard (non-profiling) jars again.
#
#   Run the command as follows before running any kiji commands
#
#   $KIJI_HOME/bin/profiling/enable-profiling.sh
#   kiji <command> etc...
#   $KIJI_HOME/bin/profiling/disable-profiling.sh

set -e

if [ -z "${KIJI_HOME}" ]; then
  echo "Please set the KIJI_HOME enviroment variable before you enable profiling."
  exit 1
fi

if [ -z "${HADOOP_HOME}" ]; then
  echo "Please set the HADOOP_HOME environment variable before you enable profiling."
  exit 1
fi

# Name of the profiled version of the KijiSchema jar
profiling_kiji_schema_jar_name="kiji-schema-${project.version}-profiling.jar"
# Name of the original KijiSchema jar
kiji_schema_jar_name="kiji-schema-${project.version}.jar"

# We may have Hadoop distribution-specific jars to load in
# $KIJI_HOME/lib/distribution/hadoopN, where N is the major digit of the Hadoop
# version. Only load at most one such set of jars.

# Detect and extract the current Hadoop version number. e.g. "Hadoop 2.x-..." -> "2"
# You can override this with $KIJI_HADOOP_DISTRO_VER (e.g. "hadoop1" or "hadoop2").
hadoop_major_version=`$HADOOP_HOME/bin/hadoop version | head -1 | cut -c 8`
if [ -z "$hadoop_major_version" -a -z "$KIJI_HADOOP_DISTRO_VER" ]; then
  echo "Warning: Unknown Hadoop version. May not be able to load all Kiji jars."
  echo "Set KIJI_HADOOP_DISTRO_VER to 'hadoop1' or 'hadoop2' to load these."
else
  KIJI_HADOOP_DISTRO_VER=${KIJI_HADOOP_DISTRO_VER:-"hadoop$hadoop_major_version"}
fi

distrodir="$KIJI_HOME/lib/distribution/$KIJI_HADOOP_DISTRO_VER"

# This name represents both profiling and non profiling jar for KijiMR
kiji_mr_jar_prefix="kiji-mapreduce-${KIJI_HADOOP_DISTRO_VER}-"

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
if [ -f "${KIJI_HOME}/lib/${kiji_schema_jar_name}" ]; then
  echo "Moving the non-profiled KijiSchema jar from "\
  "${KIJI_HOME}/lib/${kiji_schema_jar_name} to ${orig_dir}"
  mv "${KIJI_HOME}/lib/${kiji_schema_jar_name}" "${orig_dir}"
else
  echo "${KIJI_HOME}/lib/${kiji_schema_jar_name} does not exist. Is profiling" \
  " enabled?"
  inconsistent_state="true"
fi

# Copy profiling KijiSchema jar into the $KIJI_HOME/lib directory
if [ ! -f "${KIJI_HOME}/lib/${profiling_kiji_schema_jar_name}" ]; then
  echo "Moving the profiling enabled KijiSchema jar from " \
  "${KIJI_HOME}/lib/profiling/${profiling_kiji_schema_jar_name} to ${KIJI_HOME}/lib"
  cp "${KIJI_HOME}/lib/profiling/${profiling_kiji_schema_jar_name}" "${KIJI_HOME}/lib"
else
  echo "Profiling enabled jar already exists in ${KIJI_HOME}/lib. Not overwriting."
  inconsistent_state="true"
fi

# Copy the aspectj jar into the $KIJI_HOME/lib directory
if [ ! -f "${KIJI_HOME}/lib/${aspectj_jar_name}" ]; then
  echo "Moving the aspectj jar from " \
  "${KIJI_HOME}/lib/profiling/${aspectj_jar_name} to ${KIJI_HOME}/lib"
  cp "${KIJI_HOME}/lib/profiling/${aspectj_jar_name}" "${KIJI_HOME}/lib"
else
  echo "Aspectj jar already exists in ${KIJI_HOME}/lib. Not overwriting."
  inconsistent_state="true"
fi

# Check if distrodir contains a mapreduce jar that does not have profiling in its name
# Move the other kijiMR jar out of the way

# Check if distrodir exists, i.e. we are in a BentoBox
if [ -d "${distrodir}" ]; then
  has_kijimr_profile_jar="false"
  for fname in "${distrodir}/${kiji_mr_jar_prefix}"*.jar; do
    if [ "${fname}" == *"profiling.jar" ]; then
      # We found a profiling jar already installed. Set a flag so we don't
      # clobber it with another profiling jar
      has_kijimr_profile_jar="true"
    else
      echo "Moving ${fname} to ${orig_dir}"
      mv "${fname}" "${orig_dir}/"
    fi
  done

  if [ "${has_kijimr_profile_jar}" != "true" ]; then
    cp "${KIJI_HOME}/lib/profiling/${kiji_mr_jar_prefix}"* "${distrodir}"
  else
    echo "A KijiMR profiling jar is already installed at ${distrodir}. Not overwriting."
    inconsistent_state="true"
  fi
fi

if [ "${inconsistent_state}" == "false" ]; then
  echo ""
  echo "Profiling has been enabled."
  echo ""
else
  echo "Please check the error messages. Some manual actions may be required to enable" \
  "profiling."
  exit 1
fi
