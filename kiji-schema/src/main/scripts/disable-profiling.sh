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
#   The disable-profiling script performs clean up so that you can use the
#   standard (non-profiling) jars after you are done profiling.
#
#   The sequence of commands is as follows:
#
#   $KIJI_HOME/bin/enable-profiling.sh
#   kiji <command> etc...
#   $KIJI_HOME/bin/disable-profiling.sh

set -e

if [ -z "${KIJI_HOME}" ]; then
  echo "Please set the KIJI_HOME enviroment variable before you enable profiling."
  exit 1
fi

if [ -z "${HADOOP_HOME}" ]; then
  echo "Please set the HADOOP_HOME environment variable before you enable profiling."
  exit 1
fi

# Name of the profiling version of the kiji schema jar
kiji_profiling_schema_jar_name="kiji-schema-${project.version}-profiling.jar"

# Name of the original kiji schema jar
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

# The location to store the original KijiSchema and KijiMR jars during profiling
# so that they may be restored later.
orig_dir="${KIJI_HOME}/lib/original_jars"

# Flag to indicate if something unexpected was found and disabling profiling requires
# any manual intervention.
inconsistent_state="false"

# Remove the profiling jars from lib and distrodir. We have cp'd them while
# enabling profiling, so rm should be fine.
if [ -f "${KIJI_HOME}/lib/${kiji_profiling_schema_jar_name}" ]; then
  echo "Removing profile enabled kiji schema jar..."
  rm -f "${KIJI_HOME}/lib/${kiji_profiling_schema_jar_name}"
else
  echo "Did not find ${kiji_profiling_schema_jar_name} in ${KIJI_HOME}/lib. "
  echo "Is profiling enabled?"
  inconsistent_state="true"
fi

if [ -d "${distrodir}" ]; then
  if [ -f "${distrodir}/${kiji_mr_jar_prefix}"*"profiling.jar" ]; then
    echo "Remove profile enabled kiji mapreduce jar..."
    rm -f "${distrodir}/${kiji_mr_jar_prefix}"*"profiling.jar"
  else
    echo "Did not find ${kiji_mr_jar_prefix}*-profiling.jar in ${distrodir}. "
    echo "Is profiling enabled?"
    inconsistent_state="true"
  fi
fi

# Check if the orig_dir exists and move the schema and mapreduce jars into their
# rightful places
if [ -d "${orig_dir}" ]; then
  if [ ! -f "${orig_dir}/${kiji_schema_jar_name}" ]; then
    echo "Cannot find original schema jar in ${orig_dir}. Please move the jar" \
    " ${kiji_schema_jar_name} to ${KIJI_HOME}/lib"
    inconsistent_state="true"
  else
    echo "Moving ${orig_dir}/${kiji_schema_jar_name} to ${KIJI_HOME}/lib/ ..."
    mv "${orig_dir}/${kiji_schema_jar_name}" "${KIJI_HOME}/lib/"
  fi

  if [ ! -f "${orig_dir}/${kiji_mr_jar_prefix}"*".jar" ]; then
    echo "Cannot find the original KijiMR jar in ${orig_dir}. Please move the" \
    "jar named ${kiji_mr_jar_prefix}*.jar to the ${distrodir} directory."
    inconsistent_state="true"
  else
    echo "Moving ${orig_dir}/${kiji_mr_jar_prefix}-* to ${distrodir}"
    mv "${orig_dir}/${kiji_mr_jar_prefix}"* "${distrodir}"
  fi
else
  echo "Did not find ${orig_dir}. This may be because profiling was not enabled."
  echo "Ensure that ${KIJI_HOME}/lib/ and ${distrodir} have the files " \
  "${kiji_schema_jar_name} and ${kiji_mr_jar_prefix}-*.jar respectively."
  inconsistent_state="true"
fi

# Remove the directory which was holding the normal jars
if [ -d "${orig_dir}" ]; then
  rmdir "${orig_dir}"
else
  echo "Directory ${orig_dir} not found."
  inconsistent_state="true"
fi

if [ "${inconsistent_state}" == "false" ]; then
  echo ""
  echo "Profiling jars have been disabled. The normal Kiji modules have been restored."
  echo ""
else
  echo "Please check the error messages. Some manual actions may be required to disable" \
  "profiling."
  exit 1
fi
