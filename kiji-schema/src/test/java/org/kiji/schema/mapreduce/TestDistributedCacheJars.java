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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestDistributedCacheJars {
  /** Configuration variable name to store jars names in. */
  private static final String CONF_TMPJARS = "tmpjars";

  // Disable checkstyle for this variable.  It must be public to work with JUnit @Rule.
  // CSOFF: VisibilityModifierCheck
  /** A temporary directory to store jars in. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  /**
   * Pre: Requires mTempDir to be set and filled (only) with .jar files.
   * These don't need to actually be jars.
   *
   * Creates a new Job and checks that jars de-dupe.
   *
   * @throws IOException if configuration can not be created.
   */
  @Test
  public void testJarsDeDupe() throws IOException {
    // Jar list should de-dupe to {"myjar_a, "myjar_b", "myjar_0", "myjar_1"}
    Set<String> dedupedJarNames = new HashSet<String>(4);
    dedupedJarNames.add("myjar_a.jar");
    dedupedJarNames.add("myjar_b.jar");
    dedupedJarNames.add("myjar_0.jar");
    dedupedJarNames.add("myjar_1.jar");

    Job job = new Job();

    List<String> someJars = new ArrayList<String>();
    // Some unique jar names.
    someJars.add("/somepath/myjar_a.jar");
    someJars.add("/another/path/myjar_b.jar");
    someJars.add("/myjar_0.jar");

    // Duplicate jars.
    someJars.add("/another/path/myjar_b.jar");
    someJars.add("/yet/another/path/myjar_b.jar");

    job.getConfiguration().set(CONF_TMPJARS, StringUtils.join(someJars, ","));

    // Now add some duplicate jars from mTempDir.
    assertEquals(0, mTempDir.getRoot().list().length);
    createTestJars("myjar_0.jar", "myjar_1.jar");
    assertEquals(2, mTempDir.getRoot().list().length);
    DistributedCacheJars.addJarsToDistributedCache(job, mTempDir.getRoot());

    // Confirm each jar appears in de-dupe list exactly once.
    String listedJars = job.getConfiguration().get(CONF_TMPJARS);
    String[] jars = listedJars.split(",");
    for (String jar : jars) {
      // Check that path terminates in an expected jar.
      Path p = new Path(jar);
      assertTrue(dedupedJarNames.contains(p.getName()));
      dedupedJarNames.remove(p.getName());
    }
    assertEquals(0, dedupedJarNames.size());
  }

  /**
   * Adds mock jars to mTempDir.
   *
   * @param jars The names of files to create.
   * @throws IOException if the files cannot be created.
   */
  private void createTestJars(String... jars) throws IOException {
    for (String jar : jars) {
      mTempDir.newFile(jar);
    }
  }
}
