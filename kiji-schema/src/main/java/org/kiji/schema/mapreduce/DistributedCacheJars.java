/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Utility class for dealing with Java JAR files and the hadoop distributed cache.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Deprecated
public final class DistributedCacheJars {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedCacheJars.class);

  /** Configuration variable name to store jars that export to distributed cache. */
  private static final String CONF_TMPJARS = "tmpjars";

  /** No constructor for this utility class. */
  private DistributedCacheJars() {}

  /**
   * Adds the jars from a directory into the distributed cache of a job.
   *
   * @param job The job to configure.
   * @param jarDirectory A path to a directory of jar files.
   * @throws IOException If there is a problem reading from the file system.
   */
  public static void addJarsToDistributedCache(Job job, String jarDirectory) throws IOException {
    addJarsToDistributedCache(job, new File(jarDirectory));
  }

  /**
   * Adds the jars from a directory into the distributed cache of a job.
   *
   * @param job The job to configure.
   * @param jarDirectory A path to a directory of jar files.
   * @throws IOException If there is a problem reading from the file system.
   */
  public static void addJarsToDistributedCache(Job job, File jarDirectory) throws IOException {
    if (null == jarDirectory) {
      throw new IllegalArgumentException("Jar directory may not be null");
    }
    if (!jarDirectory.exists()) {
      throw new IOException("The jar directory " + jarDirectory.getPath() + " does not exist.");
    }

    List<String> allJars = new ArrayList<String>();

    // Get existing jars named in configuration.
    allJars.addAll(getJarsFromConfiguration(job.getConfiguration()));

    // Add jars from jarDirectory.
    allJars.addAll(getJarsFromDirectory(job.getConfiguration(), jarDirectory));

    // De-dupe
    List<String> deDupedJars = deDuplicateJarNames(allJars);
    job.getConfiguration().set(CONF_TMPJARS, StringUtils.join(deDupedJars, ","));
  }

  /**
   * Lists all jars in the variable tmpjars of this Configuration.
   *
   * @param conf The Configuration to get jar names from
   * @return A list of jars.
   */
  public static List<String> getJarsFromConfiguration(Configuration conf) {
    List<String> allJars = new ArrayList<String>();
    String existingJars = conf.get(CONF_TMPJARS);
    if (null != existingJars && !existingJars.isEmpty()) {
      for (String jar : existingJars.split(",")) {
        allJars.add(jar);
      }
    }
    return allJars;
  }

  /**
   * @param conf Configuration to get FileSystem from
   * @param jarDirectory The directory of jars to get.
   * @return A list of qualified paths to the jars in jarDirectory.
   * @throws IOException if there's a problem.
   */
  public static List<String> getJarsFromDirectory(Configuration conf, File jarDirectory)
      throws IOException {
    if (!jarDirectory.isDirectory()) {
      throw new IOException("Attempted to add jars from non-directory: "
          + jarDirectory.getCanonicalPath());
    }
    List<String> allJars = new ArrayList<String>();
    FileSystem fileSystem = FileSystem.getLocal(conf);
    for (File jar : jarDirectory.listFiles()) {
      if (jar.exists() && !jar.isDirectory() && jar.getName().endsWith(".jar")) {
        Path jarPath = new Path(jar.getCanonicalPath());
        String qualifiedPath = jarPath.makeQualified(fileSystem).toString();
        allJars.add(qualifiedPath);
      }
    }
    return allJars;
  }

  /**
   * Takes a list of paths and returns a list of paths with unique filenames.
   *
   * @param jarList A list of jars to de-dupe.
   * @return A de-duplicated list of jars.
   */
  public static List<String> deDuplicateJarNames(List<String> jarList) {
    Set<String> jarNames = new HashSet<String>();
    List<String> jarPaths = new ArrayList<String>();
    for (String jar : jarList) {
      Path path = new Path(jar);
      String jarName = path.getName();
      if (!jarNames.contains(jarName)) {
        jarNames.add(jarName);
        jarPaths.add(jar);
      } else {
        LOG.warn("Skipping jar at " + jar + " because " + jarName + " already added.");
      }
    }
    return jarPaths;
  }
}
