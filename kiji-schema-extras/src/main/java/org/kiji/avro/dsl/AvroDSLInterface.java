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

package org.kiji.avro.dsl;

import java.util.List;

import org.apache.avro.Schema;

/**
 * Java interface to the Avro DSL parser.
 *
 * This class is implemented by {@link JavaAvroDSL}.
 */
public interface AvroDSLInterface {
  /**
   * Parse a text IDL declaration for a single Avro type.
   *
   * @param text IDL definition to parse into an Avro schema.
   * @return the Avro schema parsed from the IDL definition.
   */
  Schema parseSchema(String text);

  /**
   * Parse a text IDL declaration for a sequence of Avro types.
   *
   * @param text IDL definition to parse into a sequence of Avro schemas.
   * @return the Avro schema sequence parsed from the IDL definition.
   */
  List<Schema> parseSchemaSequence(String text);

  /**
   * Serializes an Avro schema back to its DSL string representation.
   *
   * @param schema Avro schema to represent as a DSL Avro schema definition.
   * @return the DSL text representation of the given Avro schema.
   */
  String schemaToString(Schema schema);

  /**
   * Parse an Avro value from its DSL string representation.
   *
   * @param text Text DSL representation of an Avro value.
   * @param schema Avro schema of the value to parse.
   * @param <T> is the value type to return.
   * @return the Avro value parsed from the text representation.
   */
  <T> T parseValue(String text, Schema schema);

  /**
   * Serializes an Avro value to back to its DSL string representation.
   *
   * @param value Avro value to serialize.
   * @param schema Avro schema of the value to serialize.
   * @param <T> is the value type to return.
   * @return the DSL string representation of the Avro value.
   */
  <T> String valueToString(T value, Schema schema);
}
