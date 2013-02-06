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

import org.kiji.annotations.ApiAudience;

/** Constants used by Kiji. */
@ApiAudience.Framework
public final class KConstants {
  /** Default kiji instance name. */
  public static final String DEFAULT_INSTANCE_NAME = "default";

  /** Default Kiji URI. */
  public static final String DEFAULT_URI = "kiji://.env/" + DEFAULT_INSTANCE_NAME;

  public static final long END_OF_TIME = Long.MAX_VALUE;
  public static final long BEGINNING_OF_TIME = 0;

  /** Utility classes cannot be instantiated. */
  private KConstants() {
  }
}
