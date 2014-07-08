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

package org.kiji.schema.impl.cassandra;

import org.junit.Test;

import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiURI;

/** Tests for KijiInstaller. */
public class TestCassandraKijiInstaller {
  @Test
  public void testInstallThenUninstall() throws Exception {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-cassandra://.fake.kiji-installer/chost:1234/test").build();
    CassandraKijiInstaller.get().install(uri, null);
    CassandraKijiInstaller.get().uninstall(uri, null);
  }

  @Test(expected = KijiInvalidNameException.class)
  public void testInstallNullInstance() throws Exception {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-cassandra://.fake.kiji-installer/chost:1234/").build();
    CassandraKijiInstaller.get().install(uri, null);
  }

  @Test(expected = KijiInvalidNameException.class)
  public void testUninstallNullInstance() throws Exception {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-cassandra://.fake.kiji-installer/chost:1234/").build();
    CassandraKijiInstaller.get().uninstall(uri, null);
  }

  @Test(expected = KijiNotInstalledException.class)
  public void testUninstallMissingInstance() throws Exception {
    final KijiURI uri = KijiURI
        .newBuilder("kiji-cassandra://.fake.kiji-installer/chost:1234/anInstanceThatNeverExisted")
        .build();
    CassandraKijiInstaller.get().uninstall(uri, null);
  }
}
