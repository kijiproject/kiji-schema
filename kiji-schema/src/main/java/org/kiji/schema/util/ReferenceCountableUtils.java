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

import java.io.IOException;

import com.sun.istack.logging.Logger;

/** Utilities to work with ReferenceCountable resources. */
public final class ReferenceCountableUtils {
  private static final Logger LOG = Logger.getLogger(ReferenceCountableUtils.class);

  /**
   * Releases a given resource, logging and swallowing I/O errors if needed.
   *
   * @param resource Release this resource quietly.
   * @param <T> Type of the resource to release.
   */
  public static <T> void releaseQuietly(ReferenceCountable<T> resource) {
    if (resource == null) {
      return;
    }
    try {
      resource.release();
    } catch (IOException ioe) {
      LOG.info(String.format(
          "I/O error while releasing resource '%s' : %s", resource, ioe.getMessage()));
    }
  }

  /** Utility class cannot be instantiated. */
  private ReferenceCountableUtils() {
  }
}
