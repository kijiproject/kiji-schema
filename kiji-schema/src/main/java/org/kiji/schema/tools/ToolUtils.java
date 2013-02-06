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

package org.kiji.schema.tools;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.ObjectMapper;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ByteArrayFormatter;

/**
 * Utility class providing static methods used by command-line tools.
 */
@ApiAudience.Framework
public final class ToolUtils {
  /** Disable this constructor. */
  private ToolUtils() {}

  /** Prefix to specify an HBase row key from the command-line. */
  public static final String HBASE_ROW_KEY_SPEC_PREFIX = "hbase=";

  /** Optional prefix to specify a Kiji row key from the command-line. */
  public static final String KIJI_ROW_KEY_SPEC_PREFIX = "kiji=";

  /**
   * Parses a command-line flag specifying an entity ID.
   *
   * <li> HBase row key specifications must be prefixed with <code>"hbase=..."</code>
   * <li> Kiji row key specifications may be explicitly prefixed with <code>"kiji=..."</code> if
   *     necessary. The prefix is not always necessary.
   *
   * @param entityFlag Command-line flag specifying an entity ID.
   * @param layout Layout of the table describing the entity ID format.
   * @return the entity ID as specified in the flags.
   * @throws IOException on I/O error.
   */
  public static EntityId createEntityIdFromUserInputs(String entityFlag, KijiTableLayout layout)
      throws IOException {
    Preconditions.checkNotNull(entityFlag);

    final EntityIdFactory factory = EntityIdFactory.getFactory(layout);

    if (entityFlag.startsWith(HBASE_ROW_KEY_SPEC_PREFIX)) {
      // HBase row key specification
      final String hbaseSpec = entityFlag.substring(HBASE_ROW_KEY_SPEC_PREFIX.length());
      final byte[] hbaseRowKey = parseBytesFlag(hbaseSpec);
      return factory.getEntityIdFromHBaseRowKey(hbaseRowKey);

    } else {
      // Kiji row key specification
      final String kijiSpec = entityFlag.startsWith(KIJI_ROW_KEY_SPEC_PREFIX)
          ? entityFlag.substring(HBASE_ROW_KEY_SPEC_PREFIX.length())
          : entityFlag;

      return parseKijiRowKey(kijiSpec, factory, layout);
    }
  }

  /**
   * Parses a Kiji row key specification from a command-line flag.
   *
   * @param rowKeySpec Kiji row key specification.
   * @param factory Factory for entity IDs.
   * @param layout Layout of the table to parse the entity ID of.
   * @return the parsed entity ID.
   * @throws IOException on I/O error.
   */
  public static EntityId parseKijiRowKey(
      String rowKeySpec, EntityIdFactory factory, KijiTableLayout layout)
      throws IOException {

    final Object keysFormat = layout.getDesc().getKeysFormat();
    if (keysFormat instanceof RowKeyFormat) {
      // Former, deprecated, unformatted row key specification:
      return factory.getEntityId(rowKeySpec);

    } else if (keysFormat instanceof RowKeyFormat2) {
      final RowKeyFormat2 format = (RowKeyFormat2) keysFormat;
      switch (format.getEncoding()) {
      case RAW: return factory.getEntityIdFromHBaseRowKey(parseBytesFlag(rowKeySpec));
      case FORMATTED: return parseJsonFormattedKeySpec(rowKeySpec, format, factory);
      default:
        throw new RuntimeException(String.format(
            "Invalid layout for table '%s' with unsupported keys format: '%s'.",
            layout.getName(), format));
      }

    } else {
      throw new RuntimeException(String.format("Unknown row key format: '%s'.", keysFormat));
    }
  }

  /**
   * Converts a JSON string or integer node into a Java object (String, Integer or Long).
   *
   * @param node JSON string or integer numeric node.
   * @return the JSON value, as a String, an Integer or a Long instance.
   * @throws IOException if the JSON node is neither a string nor an integer value.
   */
  private static Object getJsonStringOrIntValue(JsonNode node) throws IOException {
    if (node.isInt() || node.isLong()) {
      return node.getNumberValue();
    } else if (node.isTextual()) {
      return node.getTextValue();
    } else {
      throw new IOException(String.format(
          "Invalid JSON value: '%s', expecting string or int.", node));
    }
  }

  /**
   * Parses a JSON formatted row key specification.
   *
   * @param json JSON specification of the formatted row key.
   *     Either a JSON ordered array, a JSON map (object), or an immediate JSON primitive.
   * @param format Row key format specification from the table layout.
   * @param factory Entity ID factory.
   * @return the parsed entity ID.
   * @throws IOException on I/O error.
   */
  public static EntityId parseJsonFormattedKeySpec(
      String json, RowKeyFormat2 format, EntityIdFactory factory)
      throws IOException {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      final JsonParser parser = new JsonFactory().createJsonParser(json)
          .enable(Feature.ALLOW_COMMENTS)
          .enable(Feature.ALLOW_SINGLE_QUOTES)
          .enable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
      final JsonNode node = mapper.readTree(parser);
      if (node.isArray()) {
        final Object[] components = new Object[node.size()];
        for (int i = 0; i < node.size(); ++i) {
          components[i] = getJsonStringOrIntValue(node.get(i));
        }
        return factory.getEntityId(components);
      } else if (node.isObject()) {
        // TODO: Implement map row key specifications:
        throw new RuntimeException("Map row key specifications are not implemented yet.");
      } else {
        return factory.getEntityId(getJsonStringOrIntValue(node));
      }

    } catch (JsonParseException jpe) {
      throw new IOException(jpe);
    }
  }

  /** Prefix to specify a sequence of bytes in hexadecimal, as in: "00dead88beefaa". */
  public static final String BYTES_SPEC_PREFIX_HEX = "hex:";

  /** Prefix to specify a sequence of bytes as a URL, as in: "URL%20encoded". */
  public static final String BYTES_SPEC_PREFIX_URL = "url:";

  /** Optional prefix to specify a sequence of bytes in UTF-8, as in: "utf-8 \x00 encoded". */
  public static final String BYTES_SPEC_PREFIX_UTF8 = "utf8:";

  // Support other encoding, eg base64 encoding?

  /**
   * Parses a command-line flag specifying a byte array.
   *
   * Valid specifications are:
   *   <li> UTF-8 encoded strings, as in "utf8:encoded \x00 text".
   *   <li> Hexadecimal sequence, with "hex:00dead88beefaa".
   *   <li> URL encoded strings, as in "url:this%20is%20a%20URL".
   *
   * UTF-8 is the default, hence the "utf8:" prefix is optional unless there is an ambiguity with
   * other prefixes.
   *
   * @param flag Command-line flag specification for a byte array.
   * @return the decoded byte array.
   * @throws IOException on I/O error.
   */
  public static byte[] parseBytesFlag(String flag) throws IOException {
    if (flag.startsWith(BYTES_SPEC_PREFIX_HEX)) {
      // Hexadecimal encoded byte array:
      return ByteArrayFormatter.parseHex(flag.substring(BYTES_SPEC_PREFIX_HEX.length()));

    } else if (flag.startsWith(BYTES_SPEC_PREFIX_URL)) {
      // URL encoded byte array:
      try {
        return URLCodec.decodeUrl(Bytes.toBytes(flag));
      } catch (DecoderException de) {
        throw new IOException(de);
      }

    } else {
      // UTF-8 encoded and escaped byte array:
      final String spec = flag.startsWith(BYTES_SPEC_PREFIX_UTF8)
          ? flag.substring(BYTES_SPEC_PREFIX_UTF8.length())
          : flag;
      return Bytes.toBytes(spec);
    }
  }
}
