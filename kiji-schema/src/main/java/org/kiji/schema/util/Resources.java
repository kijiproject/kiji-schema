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

package org.kiji.schema.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * Utility for getting/closing system resources.
 */
@ApiAudience.Private
public final class Resources {
  private static final Logger LOG = LoggerFactory.getLogger(Resources.class);

  /** Can not instantiate utility. */
  private Resources() {}


  // TODO: Allow client code to specify which class loader to use.  If null, use this Thread's
  // classloader. If set, use this instead.
  private static ClassLoader mClassLoader = null;

  /**
   * Sets the class loader to use.  If null, Thread.getContextClassLoader().
   *
   * @param loader The ClassLoader to use.
   */
  public static void setClassLoader(ClassLoader loader) {
    mClassLoader = loader;
  }

  /**
   * Returns this Thread's ContextClassLoader if none has been specified to
   * setClassLoader.  Otherwise, uses that one.
   *
   * @return The ClassLoader to use.
   */
  public static ClassLoader getClassLoader() {
    return null == mClassLoader ? Thread.currentThread().getContextClassLoader() : mClassLoader;
  }

  /**
   * Gets the named resource using the classloader, or null if it can not be found.
   *
   * @param resourceFileName The resource to load from the classpath.
   * @return The loaded resource.  If not found, returns null.
   */
  public static InputStream getSystemResource(String resourceFileName) {
    InputStream resource = getClassLoader().getResourceAsStream(resourceFileName);
    if (null == resource) {
      LOG.debug("Could not find resource: " + resourceFileName + ".  Is it on your classpath?");
    }
    return resource;
  }

  /**
   * Does the best it can to close the resource.  Logs a warning on IOException.
   *
   * @deprecated Use org.apache.commons.io.IOUtils.closeQuietly(Closeable) instead.
   *
   * @param resource The resource to close.
   */
  @Deprecated
  public static void closeResource(Closeable resource) {
    try {
      if (null != resource) {
        resource.close();
      }
    } catch (IOException ioe) {
      LOG.warn("Error closing resource: " + resource);
      LOG.warn(ioe.getMessage());
    }
  }
}
