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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * Utility for getting access to system resources.
 */
@ApiAudience.Private
public final class Resources {
  private static final Logger LOG = LoggerFactory.getLogger(Resources.class);

  /** Can not instantiate utility. */
  private Resources() {}

  /**
   * Returns the classloader to use. Currently this is this Thread's ContextClassLoader.
   *
   * @return The ClassLoader to use.
   */
  private static ClassLoader getClassLoader() {
    return Thread.currentThread().getContextClassLoader();
  }

  /**
   * Gets the named resource using the classloader, or null if it can not be found.
   * Clients should close streams returned by this method.
   *
   * @param resourceFileName The resource to load from the classpath.
   * @return The loaded resource as an InputStream.  If not found, returns null.
   */
  public static InputStream openSystemResource(String resourceFileName) {
    InputStream resource = getClassLoader().getResourceAsStream(resourceFileName);
    if (null == resource) {
      LOG.debug("Could not find resource: " + resourceFileName + ".  Is it on your classpath?");
    }
    return resource;
  }

  /**
   * Gets the named resource using the classloader, or null if it can not be found.
   * Clients should close streams returned by this method.
   *
   * @param resourceFileName The resource to load from the classpath.
   * @return The loaded resource as a BufferedReader.  If not found, returns null.
   */
  public static BufferedReader openSystemTextResource(String resourceFileName) {
    InputStream resource = getClassLoader().getResourceAsStream(resourceFileName);
    if (null == resource) {
      LOG.debug("Could not find resource: " + resourceFileName + ".  Is it on your classpath?");
      return null;
    }
    try {
      return new BufferedReader(new InputStreamReader(resource, "UTF-8"));
    } catch (UnsupportedEncodingException uee) {
      // Shouldn't get here; UTF-8 is a Java universal requirement.
      LOG.debug("Could not use UTF-8 charset to open this resource?");
      return null;
    }
  }
}
