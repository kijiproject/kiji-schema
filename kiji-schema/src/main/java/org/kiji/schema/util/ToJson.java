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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import org.kiji.annotations.ApiAudience;

/**
 * Encode an Avro record into JSON.
 */
@ApiAudience.Private
public final class ToJson {

  /** Utility class cannot be instantiated. */
  private ToJson() {
  }

  private static final JsonFactory JSON_FACTORY = new JsonFactory();
  private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

  /**
   * Serializes a Java Avro value into JSON.
   *
   * When serializing records, fields whose value matches the fields' default value are omitted.
   *
   * @param value the Java value to serialize.
   * @param schema Avro schema of the value to serialize.
   * @return the value encoded as a JSON tree.
   * @throws IOException on error.
   */
  public static JsonNode toJsonNode(Object value, Schema schema) throws IOException {
    switch (schema.getType()) {
    case NULL:
      return JSON_NODE_FACTORY.nullNode();
    case BOOLEAN:
      return JSON_NODE_FACTORY.booleanNode((Boolean) value);
    case DOUBLE:
      return JSON_NODE_FACTORY.numberNode((Double) value);
    case FLOAT:
      return JSON_NODE_FACTORY.numberNode((Float) value);
    case INT:
      return JSON_NODE_FACTORY.numberNode((Integer) value);
    case LONG:
      return JSON_NODE_FACTORY.numberNode((Long) value);
    case STRING:
      return JSON_NODE_FACTORY.textNode((String) value);

    case ENUM:
      // Enums are represented as strings:
      @SuppressWarnings("rawtypes")
      final Enum enumValue = (Enum) value;
      return JSON_NODE_FACTORY.textNode(enumValue.toString());

    case BYTES:
    case FIXED:
      // TODO Bytes are represented as strings...
      throw new RuntimeException("toJsonNode(byte array) not implemented");

    case ARRAY: {
      final ArrayNode jsonArray = JSON_NODE_FACTORY.arrayNode();
      @SuppressWarnings("unchecked")
      final Iterable<Object> javaArray = (Iterable<Object>) value;
      for (Object element : javaArray) {
        jsonArray.add(toJsonNode(element, schema.getElementType()));
      }
      return jsonArray;
    }
    case MAP: {
      final ObjectNode jsonObject = JSON_NODE_FACTORY.objectNode();
      @SuppressWarnings("unchecked")
      final Map<String, Object> javaMap = (Map<String, Object>) value;
      for (Map.Entry<String, Object> entry : javaMap.entrySet()) {
        jsonObject.put(entry.getKey(), toJsonNode(entry.getValue(), schema.getValueType()));
      }
      return jsonObject;
    }

    case RECORD: {
      final ObjectNode jsonObject = JSON_NODE_FACTORY.objectNode();
      final IndexedRecord record = (IndexedRecord) value;
      if (!record.getSchema().equals(schema)) {
        throw new IOException(String.format(
            "Avro schema specifies record type '%s' but got '%s'.",
            schema.getFullName(), record.getSchema().getFullName()));
      }
      for (Schema.Field field : schema.getFields()) {
        final Object fieldValue = record.get(field.pos());
        final JsonNode fieldNode = toJsonNode(fieldValue, field.schema());
        // Outputs the field only if its value differs from the field's default:
        if ((field.defaultValue() == null) || !fieldNode.equals(field.defaultValue())) {
          jsonObject.put(field.name(), fieldNode);
        }
      }
      return jsonObject;
    }
    case UNION: return toUnionJsonNode(value, schema);

    default:
      throw new RuntimeException(String.format("Unexpected schema type '%s'.", schema));
    }
  }

  /**
   * Encodes an Avro union into a JSON node.
   *
   * @param value an Avro union to encode.
   * @param schema schema of the union to encode.
   * @return the encoded value as a JSON node.
   * @throws IOException on error.
   */
  private static JsonNode toUnionJsonNode(Object value, Schema schema) throws IOException {
    Preconditions.checkArgument(schema.getType() == Schema.Type.UNION);

    final Schema optionalType = AvroUtils.getOptionalType(schema);
    if (null != optionalType) {
      return (null == value)
          ? JSON_NODE_FACTORY.nullNode()
          : toJsonNode(value, optionalType);
    }

    final Map<Schema.Type, List<Schema>> typeMap = Maps.newEnumMap(Schema.Type.class);
    for (Schema type : schema.getTypes()) {
      List<Schema> typeList = typeMap.get(type.getType());
      if (null == typeList) {
        typeList = Lists.newArrayList();
        typeMap.put(type.getType(), typeList);
      }
      typeList.add(type);
    }

    //  null is shortened as an immediate JSON null:
    if (null == value) {
      if (!typeMap.containsKey(Schema.Type.NULL)) {
        throw new IOException(String.format("Avro schema specifies '%s' but got 'null'.", schema));
      }
      return JSON_NODE_FACTORY.nullNode();
    }

    final ObjectNode union = JSON_NODE_FACTORY.objectNode();
    for (Schema type : schema.getTypes()) {
      try {
        final JsonNode actualNode = toJsonNode(value, type);
        union.put(type.getFullName(), actualNode);
        return union;
      } catch (IOException ioe) {
        // This type was not the correct union case, ignore...
      }
    }
    throw new IOException(String.format("Unable to encode '%s' as union '%s'.",
        value, schema));
  }

  /**
   * Encodes an Avro value into a JSON string.
   *
   * Fields with default values are omitted.
   *
   * @param value Avro value to encode.
   * @param schema Avro schema of the value.
   * @return Pretty string representation of the JSON-encoded value.
   * @throws IOException on error.
   */
  public static String toJsonString(Object value, Schema schema) throws IOException {
    final JsonNode node = ToJson.toJsonNode(value, schema);
    final StringWriter stringWriter = new StringWriter();
    final JsonGenerator generator = JSON_FACTORY.createJsonGenerator(stringWriter);
    // We have disabled this because we used unions to represent row key formats
    // in the table layout. This is a HACK and needs a better solution.
    // TODO: Find better solution. https://jira.kiji.org/browse/SCHEMA-174
    //generator.disable(Feature.QUOTE_FIELD_NAMES);
    generator.setPrettyPrinter(new DefaultPrettyPrinter());
    final ObjectMapper mapper = new ObjectMapper();
    mapper.writeValue(generator, node);
    return stringWriter.toString();
  }

  /**
   * Encodes an Avro record into JSON.
   *
   * @param record Avro record to encode.
   * @return Pretty JSON representation of the record.
   * @throws IOException on error.
   */
  public static String toJsonString(IndexedRecord record) throws IOException {
    final Schema schema = record.getSchema();
    return toJsonString(record, schema);
  }

  /**
   * Standard Avro/JSON encoder.
   *
   * @param value Avro value to encode.
   * @param schema Avro schema of the value.
   * @return JSON-encoded value.
   * @throws IOException on error.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static String toAvroJsonString(Object value, Schema schema) throws IOException {
    try {
      final ByteArrayOutputStream jsonOutputStream = new ByteArrayOutputStream();
      final JsonEncoder jsonEncoder =
          EncoderFactory.get().jsonEncoder(schema, jsonOutputStream);
      final GenericDatumWriter writer = new GenericDatumWriter(schema);
      writer.write(value, jsonEncoder);
      jsonEncoder.flush();
      return Bytes.toString(jsonOutputStream.toByteArray());
    } catch (IOException ioe) {
      throw new RuntimeException("Internal error: " + ioe);
    }
  }

  /**
   * Standard Avro/JSON encoder.
   *
   * @param record Avro record to encode.
   * @return JSON-encoded value.
   * @throws IOException on error.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static String toAvroJsonString(IndexedRecord record) throws IOException {
    final Schema schema = record.getSchema();
    try {
      final ByteArrayOutputStream jsonOutputStream = new ByteArrayOutputStream();
      final JsonEncoder jsonEncoder =
          EncoderFactory.get().jsonEncoder(schema, jsonOutputStream);

      final SpecificDatumWriter writer = new SpecificDatumWriter(record.getClass());
      writer.write(record, jsonEncoder);
      jsonEncoder.flush();
      return Bytes.toString(jsonOutputStream.toByteArray());
    } catch (IOException ioe) {
      throw new RuntimeException("Internal error: " + ioe);
    }

  }
}
