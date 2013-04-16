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

package org.kiji.schema.filter;

import org.codehaus.jackson.JsonNode;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Responsible for deserializing KijiRowFilters from the results of a call to
 * {@link KijiRowFilter#toJson}.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Extensible
public interface KijiRowFilterDeserializer {
  /**
   * Deserialize JSON into a {@code KijiRowFilter}.
   *
   * @param root the {@code JsonNode} holding the contents of the filter
   * @return a fully populated and operable {@code KijiRowFilter}
   */
  KijiRowFilter createFromJson(JsonNode root);
}
