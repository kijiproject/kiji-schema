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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.hbase.HBaseFactory;

/** Installs or uninstalls Kiji instances. */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public class KijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(KijiInstaller.class);

  /** Singleton KijiInstaller. **/
  private static final KijiInstaller SINGLETON = new KijiInstaller();

  /** Constructs a KijiInstaller. */
  protected KijiInstaller() {
  }

  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the Kiji instance name is invalid or already exists.
   */
  public void install(final KijiURI uri, final Configuration conf) throws IOException {
    install(uri, HBaseFactory.Provider.get(), Collections.<String, String>emptyMap(), conf);
  }

  /**
   * Uninstalls the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to uninstall.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid or already exists.
   */
  public void uninstall(final KijiURI uri, final Configuration conf) throws IOException {
    uninstall(uri, HBaseFactory.Provider.get(), conf);
  }

  /**
   * Installs a Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param hbaseFactory Factory for HBase instances.
   * @param properties Map of the initial system properties for installation, to be used in addition
   *     to the defaults.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid or already exists.
   */
  public void install(
      final KijiURI uri,
      final HBaseFactory hbaseFactory,
      final Map<String, String> properties,
      final Configuration conf
  ) throws IOException {
    // pseudo-abstract method
    // This method is required to be overridden by subclasses, and KijiURI.getKijiInstaller is
    // required to return a strict subclass of KijiInstaller, so this is not a loop.
    uri.getKijiInstaller().install(uri, hbaseFactory, properties, conf);
  }

  /**
   * Removes a Kiji instance including any user tables.
   *
   * @param uri URI of the Kiji instance to uninstall.
   * @param hbaseFactory Factory for HBase instances.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid.
   * @throws KijiNotInstalledException if the specified instance does not exist.
   */
  public void uninstall(
      final KijiURI uri,
      final HBaseFactory hbaseFactory,
      final Configuration conf
  ) throws IOException {
    // pseudo-abstract method
    // This method is required to be overridden by subclasses, and KijiURI.getKijiInstaller is
    // required to return a strict subclass of KijiInstaller, so this is not a loop.
    uri.getKijiInstaller().uninstall(uri, hbaseFactory, conf);
  }

  /**
   * Gets an instance of a KijiInstaller.
   *
   * @return An instance of a KijiInstaller.
   */
  public static KijiInstaller get() {
    return SINGLETON;
  }
}
