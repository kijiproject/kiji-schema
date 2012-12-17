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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.impl.HBaseAdminFactory;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.util.LockFactory;

/** Factory for HBase instances based on URIs. */
@ApiAudience.Framework
public interface HBaseFactory {

  /** Provider for the default HBaseFactory. */
  public static final class Provider {
    private static final Logger LOG = LoggerFactory.getLogger(Provider.class);

    /** HBaseFactory instance. */
    private static final HBaseFactory INSTANCE = getInternal();

    /** @return the default HBaseFactory. */
    public static HBaseFactory get() {
      return INSTANCE;
    }

    /**
     * Looks up the default HBaseFactory.
     *
     * Use the TestingHBaseFactory if available, falls back on DefaultHBaseFactory otherwise.
     *
     * @return the default HBaseFactory.
     */
    private static HBaseFactory getInternal() {
      // TODO: Rewrite using org.kiji.delegation.Lookup.getPriority()
      try {
        final Class<?> testingClass = Class.forName("org.kiji.schema.TestingHBaseFactory");
        try {
          final Method method = testingClass.getMethod("get");
          return (HBaseFactory) method.invoke(null);
        } catch (NoSuchMethodException nsme) {
          throw new RuntimeException(nsme);
        } catch (InvocationTargetException ite) {
          throw new RuntimeException(ite);
        } catch (IllegalAccessException iae) {
          throw new RuntimeException(iae);
        }
      } catch (ClassNotFoundException cnfe) {
        LOG.debug("Testing HBaseFactory not found.");
      }
      return HBaseFactory.Provider.get();
    }

    /** Utility class may not be instantiated. */
    private Provider() {
    }
  }

  /**
   * Reports a factory for HTableInterface for a given HBase instance.
   *
   * @param uri URI of the HBase instance to work with.
   * @return a factory for HTableInterface for the specified HBase instance.
   */
  HTableInterfaceFactory getHTableInterfaceFactory(KijiURI uri);

  /**
   * Reports a factory for HBaseAdmin for a given HBase instance.
   *
   * @param uri URI of the HBase instance to work with.
   * @return a factory for HBaseAdmin for the specified HBase instance.
   */
  HBaseAdminFactory getHBaseAdminFactory(KijiURI uri);

  /**
   * Creates a lock factory for a given Kiji instance.
   *
   * @param uri URI of the Kiji instance to create a lock factory for.
   * @param conf Hadoop configuration.
   * @return a factory for locks for the specified Kiji instance.
   * @throws IOException on I/O error.
   */
  LockFactory getLockFactory(KijiURI uri, Configuration conf) throws IOException;
}
