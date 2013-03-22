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

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Interface for reference-countable resources.
 *
 * @param <T> Type of the resource. Must implement ReferenceCountable.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public interface ReferenceCountable<T extends ReferenceCountable<T>> {
  /**
   * Expresses interest in retaining this resource.
   *
   * The resource will not be disposed until a call to release() happens
   *
   * @return the retained resource.
   */
  T retain();

  /**
   * Notifies this resource that we are no longer interested in it.
   *
   * This resource may be disposed if no other entity has expressed interest in using it.
   *
   * @throws IOException on I/O error.
   */
  void release() throws IOException;
}
