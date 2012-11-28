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

/** Thrown when attempting to open a non existing/not installed Kiji instance. */
public class KijiNotInstalledException extends RuntimeException {
  /** The instance name of the missing Kiji instance. */
  private final String mInstanceName;

  /**
   * Creates a new <code>KijiNotInstalledException</code> with the specified
   * detail message.
   *
   * @param message The exception message.
   * @param instanceName the Kiji instance name that is not installed.
   */
  public KijiNotInstalledException(String message, String instanceName) {
    super(message);
    mInstanceName = instanceName;
  }

  /** @return the name of the missing Kiji instance. */
  public String getInstanceName() {
    return mInstanceName;
  }
}
