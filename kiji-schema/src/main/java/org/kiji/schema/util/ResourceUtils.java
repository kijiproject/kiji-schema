/**
 * (c) Copyright 2013 WibiData, Inc.
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

import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities to work with ReferenceCountable resources. */
public final class ResourceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

  /**
   * Closes the specified resource, logging and swallowing I/O errors if needed.
   *
   * @param resource Close this resource.
   */
  public static void closeOrLog(Closeable resource) {
    try {
      closeIfNotNull(resource);
    } catch (IOException ioe) {
      LOG.warn("I/O error while closing resource '{}':\n{}",
          resource, StringUtils.stringifyException(ioe));
    }
  }

  /**
   * Releases the specified resource, logging and swallowing I/O errors if needed.
   *
   * @param resource Release this resource.
   * @param <T> Type of the resource to release.
   */
  public static <T> void releaseOrLog(ReferenceCountable<T> resource) {
    try {
      releaseIfNotNull(resource);
    } catch (IOException ioe) {
      LOG.warn("I/O error while releasing resource '{}':\n{}",
          resource, StringUtils.stringifyException(ioe));
    }
  }

  /**
   * Closes the specified resource if it isn't null.
   *
   * @param resource Close this resource.
   * @throws IOException If there is an error closing the resource.
   */
  public static void closeIfNotNull(Closeable resource) throws IOException {
    if (resource == null) {
      return;
    }
    resource.close();
  }

  /**
   * Releases the specified resource if it isn't null.
   *
   * @param resource Release this resource.
   * @param <T> Type of the resource to release.
   * @throws IOException If there is an error closing the resource.
   */
  public static <T> void releaseIfNotNull(ReferenceCountable<T> resource) throws IOException {
    if (resource == null) {
      return;
    }
    resource.release();
  }

  /** Utility class cannot be instantiated. */
  private ResourceUtils() {
  }
}
