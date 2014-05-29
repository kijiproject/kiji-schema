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

import java.io.IOException;

import org.apache.avro.Schema;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.avro.AvroSchema;

/** Resolves AvroSchema descriptors into Schema objects through a {@link KijiSchemaTable}. */
@ApiAudience.Private
public final class SchemaTableAvroResolver implements AvroSchemaResolver {
  /** Schema table used to resolve Avro schemas. */
  private final KijiSchemaTable mSchemaTable;

  /**
   * Creates a new Avro schema resolver.
   *
   * @param schemaTable Resolve schema UIDs against this schema table.
   */
  public SchemaTableAvroResolver(final KijiSchemaTable schemaTable) {
    mSchemaTable = schemaTable;
  }

  /** {@inheritDoc} */
  @Override
  public Schema apply(AvroSchema avroSchema) {
    if (avroSchema.getJson() != null) {
      return new Schema.Parser().parse(avroSchema.getJson());
    } else if (avroSchema.getUid() != null) {
      try {
        final Schema schema = mSchemaTable.getSchema(avroSchema.getUid());
        if (schema == null) {
          throw new KijiIOException(String.format(
              "Schema UID %d unknown in Kiji instance '%s'.",
              avroSchema.getUid(), mSchemaTable));
        }
        return schema;
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }
    } else {
      throw new KijiIOException(
          "AvroSchema neither has a schema UID nor a schema JSON descriptor.");
    }
  }
}
