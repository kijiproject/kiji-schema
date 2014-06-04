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

package org.kiji.schema;

import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

/** Tests for KijiInstaller. */
public class TestKijiInstaller extends KijiClientTest {
  @Test
  public void testInstallThenUninstall() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final KijiURI uri = KijiURI.newBuilder("kiji://.fake.kiji-installer/test").build();
    KijiInstaller.get().install(uri, conf);
    KijiInstaller.get().uninstall(uri, conf);
  }

  @Test
  public void testInstallNullInstance() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final KijiURI uri = KijiURI.newBuilder("kiji://.fake.kiji-installer/").build();
    try {
      KijiInstaller.get().install(uri, conf);
      fail("An exception should have been thrown.");
    } catch (KijiInvalidNameException kine) {
      assertEquals(
          "Kiji URI 'kiji://.fake.kiji-installer:2181/' does not specify a Kiji instance name",
          kine.getMessage());
    }
  }

  @Test
  public void testUninstallNullInstance() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final KijiURI uri = KijiURI.newBuilder("kiji://.fake.kiji-installer/").build();
    try {
      KijiInstaller.get().uninstall(uri, conf);
      fail("An exception should have been thrown.");
    } catch (KijiInvalidNameException kine) {
      assertEquals(
          "Kiji URI 'kiji://.fake.kiji-installer:2181/' does not specify a Kiji instance name",
          kine.getMessage());
    }
  }

  @Test
  public void testUninstallMissingInstance() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final KijiURI uri =
        KijiURI.newBuilder("kiji://.fake.kiji-installer/anInstanceThatNeverExisted").build();
    try {
      KijiInstaller.get().uninstall(uri, conf);
      fail("An exception should have been thrown.");
    } catch (KijiNotInstalledException knie) {
      assertTrue(Pattern.matches(
          "Kiji instance kiji://.*/anInstanceThatNeverExisted/ is not installed\\.",
          knie.getMessage()));
    }
  }

  @Test
  public void testUninstallingInstanceWithUsersDoesNotFail() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final KijiURI uri = KijiURI.newBuilder("kiji://.fake.kiji-installer/test").build();
    final KijiInstaller installer = KijiInstaller.get();
    installer.install(uri, conf);
    Kiji kiji = Kiji.Factory.get().open(uri);
    try {
      installer.uninstall(uri, conf);
    } finally {
      kiji.release();
    }
  }
}
