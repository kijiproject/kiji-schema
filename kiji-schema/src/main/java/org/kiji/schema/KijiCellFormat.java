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

import org.kiji.schema.avro.SchemaStorage;


/** Kiji cell encoding format. */
public enum KijiCellFormat {
  /** Encoded data is prepended with the Avro schema MD5 hash (16 bytes). */
  HASH,

  /** Encoded data is prepended with the unique ID of the Avro schema (variable length). */
  UID,

  /** Data is encoded without a reference to the Avro schema (see WIBI-1528). */
  NONE;

  /**
   * Converts a schema storage into a Kiji cell encoding.
   *
   * @param storage Schema storage.
   * @return Kiji cell encoding format.
   */
  public static KijiCellFormat fromSchemaStorage(SchemaStorage storage) {
    switch (storage) {
    case HASH: return HASH;
    case UID: return UID;
    case FINAL: return NONE;
    default: throw new RuntimeException("Dead code");
    }
  }
}
