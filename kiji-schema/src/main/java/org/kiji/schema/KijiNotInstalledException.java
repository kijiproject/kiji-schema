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

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/** Thrown when attempting to open a non existing/not installed Kiji instance. */
@ApiAudience.Public
@ApiStability.Stable
public final class KijiNotInstalledException extends RuntimeException {
  /** The instance name of the missing Kiji instance. */
  private final String mInstanceName;

  /** The URI of the missing Kiji instance. */
  private final KijiURI mURI;

  /**
   * Creates a new <code>KijiNotInstalledException</code> with the specified
   * detail message.
   *
   * @param message The exception message.
   * @param kijiURI The URI for the uninstalled instance.
   */
   public KijiNotInstalledException(String message, KijiURI kijiURI) {
    super(message);
    mURI = kijiURI;
    mInstanceName = null;
  }

  /**
   * Returns the URI of the missing Kiji instance.
   * @return the URI of the missing Kiji instance.
   */
  public KijiURI getURI() {
    return mURI;
  }

  /**
   * Creates a new <code>KijiNotInstalledException</code> with the specified
   * detail message.
   *
   * @param message The exception message.
   * @param instanceName The Kiji instance name that is not installed.
   * @deprecated Use {@link KijiNotInstalledException#KijiNotInstalledException(String, String)}.
   */
  public KijiNotInstalledException(String message, String instanceName) {
    super(message);
    mInstanceName = instanceName;
    mURI = null;
  }

  /**
   * Returns the name of the missing Kiji instance.
   * @return the name of the missing Kiji instance.
   */
  public String getInstanceName() {
    if (mURI == null) {
      Preconditions.checkNotNull(mInstanceName != null);
      return mInstanceName;
    } else {
      return mURI.getInstance();
    }
  }
}
