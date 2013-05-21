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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import org.kiji.annotations.ApiAudience;

/**
 * Decode a JSON string into an Avro record.
 */
@ApiAudience.Private
public final class FromJson {

  /** Utility class cannot be instantiated. */
  private FromJson() {
  }

  /**
   * Decodes a JSON node as an Avro value.
   *
   * Comply with specified default values when decoding records with missing fields.
   *
   * @param json JSON node to decode.
   * @param schema Avro schema of the value to decode.
   * @return the decoded value.
   * @throws IOException on error.
   */
  public static Object fromJsonNode(JsonNode json, Schema schema) throws IOException {
    switch (schema.getType()) {
    case INT: {
      if (!json.isInt()) {
        throw new IOException(String.format(
            "Avro schema specifies '%s' but got JSON value: '%s'.",
            schema, json));
      }
      return json.getIntValue();
    }
    case LONG: {
      if (!json.isLong() && !json.isInt()) {
        throw new IOException(String.format(
            "Avro schema specifies '%s' but got JSON value: '%s'.",
            schema, json));
      }
      return json.getLongValue();
    }
    case FLOAT: {
      if (json.isDouble() || json.isInt() || json.isLong()) {
        return (float) json.getDoubleValue();
      }
      throw new IOException(String.format(
          "Avro schema specifies '%s' but got JSON value: '%s'.",
          schema, json));
    }
    case DOUBLE: {
      if (json.isDouble() || json.isInt() || json.isLong()) {
        return json.getDoubleValue();
      }
      throw new IOException(String.format(
          "Avro schema specifies '%s' but got JSON value: '%s'.",
          schema, json));
    }
    case STRING: {
      if (!json.isTextual()) {
        throw new IOException(String.format(
            "Avro schema specifies '%s' but got JSON value: '%s'.",
            schema, json));
      }
      return json.getTextValue();
    }
    case BOOLEAN: {
      if (!json.isBoolean()) {
        throw new IOException(String.format(
            "Avro schema specifies '%s' but got JSON value: '%s'.",
            schema, json));
      }
      return json.getBooleanValue();
    }

    case ARRAY: {
      if (!json.isArray()) {
        throw new IOException(String.format(
            "Avro schema specifies '%s' but got JSON value: '%s'.",
            schema, json));
      }
      final List<Object> list = Lists.newArrayList();
      final Iterator<JsonNode> it = json.getElements();
      while (it.hasNext()) {
        final JsonNode element = it.next();
        list.add(fromJsonNode(element, schema.getElementType()));
      }
      return list;
    }
    case MAP: {
      if (!json.isObject()) {
        throw new IOException(String.format(
            "Avro schema specifies '%s' but got JSON value: '%s'.",
            schema, json));
      }
      assert json instanceof ObjectNode; // Help findbugs out.
      final Map<String, Object> map = Maps.newHashMap();
      final Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) json).getFields();
      while (it.hasNext()) {
        final Map.Entry<String, JsonNode> entry = it.next();
        map.put(entry.getKey(), fromJsonNode(entry.getValue(), schema.getValueType()));
      }
      return map;
    }

    case RECORD: {
      if (!json.isObject()) {
        throw new IOException(String.format(
            "Avro schema specifies '%s' but got JSON value: '%s'.",
            schema, json));
      }
      final Set<String> fields = Sets.newHashSet(json.getFieldNames());
      final SpecificRecord record = newSpecificRecord(schema.getFullName());
      for (Schema.Field field : schema.getFields()) {
        final String fieldName = field.name();
        final JsonNode fieldElement = json.get(fieldName);
        if (fieldElement != null) {
          final Object fieldValue = fromJsonNode(fieldElement, field.schema());
          record.put(field.pos(), fieldValue);
        } else if (field.defaultValue() != null) {
          record.put(field.pos(), fromJsonNode(field.defaultValue(), field.schema()));
        } else {
          throw new IOException(String.format(
              "Error parsing Avro record '%s' with missing field '%s'.",
              schema.getFullName(), field.name()));
        }
        fields.remove(fieldName);
      }
      if (!fields.isEmpty()) {
        throw new IOException(String.format(
            "Error parsing Avro record '%s' with unexpected fields: %s.",
            schema.getFullName(), Joiner.on(",").join(fields)));
      }
      return record;
    }

    case UNION: return fromUnionJsonNode(json, schema);

    case NULL: {
      if (!json.isNull()) {
        throw new IOException(String.format(
            "Avro schema specifies '%s' but got JSON value: '%s'.",
            schema, json));
      }
      return null;
    }

    case BYTES:
    case FIXED: {
      if (!json.isTextual()) {
        throw new IOException(String.format(
            "Avro schema specifies '%s' but got non-string JSON value: '%s'.",
            schema, json));
      }
      // TODO: parse string into byte array.
      throw new RuntimeException("Parsing byte arrays is not implemented yet");
    }

    case ENUM: {
      if (!json.isTextual()) {
        throw new IOException(String.format(
            "Avro schema specifies enum '%s' but got non-string JSON value: '%s'.",
            schema, json));
      }
      final String enumValStr = json.getTextValue();
      return enumValue(schema.getFullName(), enumValStr);
    }

    default:
      throw new RuntimeException("Unexpected schema type: " + schema);
    }
  }

  /**
   * Decodes a union from a JSON node.
   *
   * @param json JSON node to decode.
   * @param schema Avro schema of the union value to decode.
   * @return the decoded value.
   * @throws IOException on error.
   */
  private static Object fromUnionJsonNode(JsonNode json, Schema schema) throws IOException {
    Preconditions.checkArgument(schema.getType() == Schema.Type.UNION);

    try {
      final Schema optionalType = AvroUtils.getOptionalType(schema);
      if (optionalType != null) {
        return json.isNull() ? null : fromJsonNode(json, optionalType);
      }
    } catch (IOException ioe) {
      // Union value may be wrapped, ignore.
    }

    /** Map from Avro schema type to list of schemas of this type in the union. */
    final Map<Schema.Type, List<Schema>> typeMap = Maps.newEnumMap(Schema.Type.class);
    for (Schema type : schema.getTypes()) {
      List<Schema> types = typeMap.get(type.getType());
      if (null == types) {
        types = Lists.newArrayList();
        typeMap.put(type.getType(), types);
      }
      types.add(type);
    }

    if (json.isObject() && (json.size() == 1)) {
      final Map.Entry<String, JsonNode> entry = json.getFields().next();
      final String typeName = entry.getKey();
      final JsonNode actualNode = entry.getValue();

      for (Schema type : schema.getTypes()) {
        if (type.getFullName().equals(typeName)) {
          return fromJsonNode(actualNode, type);
        }
      }
    }

    for (Schema type : schema.getTypes()) {
      try {
        return fromJsonNode(json, type);
      } catch (IOException ioe) {
        // Wrong union type case.
      }
    }

    throw new IOException(String.format(
        "Unable to decode JSON '%s' for union '%s'.",
        json, schema));
  }

  /**
   * Decodes a JSON encoded record.
   *
   * @param json JSON tree to decode, encoded as a string.
   * @param schema Avro schema of the value to decode.
   * @return the decoded value.
   * @throws IOException on error.
   */
  public static Object fromJsonString(String json, Schema schema) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonParser parser = new JsonFactory().createJsonParser(json)
        .enable(Feature.ALLOW_COMMENTS)
        .enable(Feature.ALLOW_SINGLE_QUOTES)
        .enable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
    final JsonNode root = mapper.readTree(parser);
    return fromJsonNode(root, schema);
  }

  /**
   * Instantiates a specific record by name.
   *
   * @param fullName Fully qualified record name to instantiate.
   * @return a brand-new specific record instance of the given class.
   * @throws IOException on error.
   */
  private static SpecificRecord newSpecificRecord(String fullName) throws IOException {
    try {
      @SuppressWarnings("rawtypes")
      final Class klass = Class.forName(fullName);
      return (SpecificRecord) klass.newInstance();
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(String.format(
          "Error while deserializing JSON: '%s' class not found.", fullName));
    } catch (IllegalAccessException iae) {
      throw new IOException(String.format(
          "Error while deserializing JSON: cannot access '%s'.", fullName));
    } catch (InstantiationException ie) {
      throw new IOException(String.format(
          "Error while deserializing JSON: cannot instantiate '%s'.", fullName));
    }
  }

  /**
   * Looks up an Avro enum by name and string value.
   *
   * @param fullName Fully qualified enum name to look-up.
   * @param value Enum value as a string.
   * @return the Java enum value.
   * @throws IOException on error.
   */
  @SuppressWarnings("unchecked")
  private static Object enumValue(String fullName, String value) throws IOException {
    try {
      @SuppressWarnings("rawtypes")
      final Class enumClass = Class.forName(fullName);
      return Enum.valueOf(enumClass, value);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(String.format(
          "Error while deserializing JSON: '%s' enum class not found.", fullName));
    }
  }

  /**
   * Standard Avro JSON decoder.
   *
   * @param json JSON string to decode.
   * @param schema Schema of the value to decode.
   * @return the decoded value.
   * @throws IOException on error.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static Object fromAvroJsonString(String json, Schema schema) throws IOException {
    final InputStream jsonInput = new ByteArrayInputStream(json.getBytes("UTF-8"));
    final Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonInput);
    final SpecificDatumReader reader = new SpecificDatumReader(schema);
    return reader.read(null, decoder);
  }

}
