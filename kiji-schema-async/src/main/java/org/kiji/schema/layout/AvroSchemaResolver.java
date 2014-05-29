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

package org.kiji.schema.layout;

import com.google.common.base.Function;
import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.avro.AvroSchema;

/** Resolves AvroSchema descriptors into Schema objects. */

/** Interface for functions that resolve AvroSchema descriptors into Schema objects. */
@ApiAudience.Private
public interface AvroSchemaResolver extends Function<AvroSchema, Schema> {
  /**
   * Resolves an Avro schema descriptor into a Schema object.
   *
   * @param avroSchema Avro schema descriptor to resolve.
   * @return the resolved Schema object.
   */
  @Override
  Schema apply(AvroSchema avroSchema);
}
