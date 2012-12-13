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

import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;

/**
 * General purpose Avro utilities.
 */
@ApiAudience.Private
public final class AvroUtils {

  /** Utility class cannot be instantiated. */
  private AvroUtils() {
  }

  /**
   * Reports whether the given schema is an optional type (ie. a union { null, Type }).
   *
   * @param schema The schema to test.
   * @return the optional type, if the specified schema describes an optional type, null otherwise.
   */
  public static Schema getOptionalType(Schema schema) {
    Preconditions.checkArgument(schema.getType() == Schema.Type.UNION);
    final List<Schema> types = schema.getTypes();
    if (types.size() != 2) {
      return null;
    }
    if (types.get(0).getType() == Schema.Type.NULL) {
      return types.get(1);
    } else if (types.get(1).getType() == Schema.Type.NULL) {
      return types.get(0);
    } else {
      return null;
    }
  }

}
