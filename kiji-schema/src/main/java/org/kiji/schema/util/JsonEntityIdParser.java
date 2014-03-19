/**
 * (c) Copyright 2014 WibiData, Inc.
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

import com.google.common.base.Preconditions;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.ObjectMapper;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.HashSpec;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Container class for entity ids which can be backed as strings
 * (suitable for raw, hashed, hash-prefixed, and materialization suppressed keys)
 * xor as a list of components
 * (suitable for formatted entity ids without materialization unsuppresed).
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class JsonEntityIdParser {
  private static final ObjectMapper BASIC_MAPPER = new ObjectMapper();

  /**
   * Prefixes for specifying hex row keys.
   */
  public static final String HBASE_ROW_KEY_PREFIX = "hbase=";
  public static final String HBASE_HEX_ROW_KEY_PREFIX = "hbase_hex=";

  /**
   * Back eid as either string or list of components.
   */
  private final String mStringEntityId;
  private final Object[] mComponents;

  /**
   * This field is only relevant w.r.t. wildcarded lists of components.
   */
  private final boolean mIsWildcarded;

  /** The table layout associated with this parser. **/
  private KijiTableLayout mLayout;

  /**
   * Private constructor for JsonEntityIdParser parametrized by a String.
   * Validate fields as necessary.
   *
   * @param stringEntityId string representing the entity id.
   * @param layout is the table's layout.
   */
  private JsonEntityIdParser(
      final String stringEntityId,
      final KijiTableLayout layout) {
    // stringEntityId may not be null.
    Preconditions.checkNotNull(stringEntityId, "Incoming entity_id can't be null.");
    // StringEntityId must be prefixed by "hbase=" or "hbase_hex=".
    Preconditions.checkArgument(stringEntityId.startsWith(HBASE_ROW_KEY_PREFIX)
        || stringEntityId.startsWith(HBASE_HEX_ROW_KEY_PREFIX),
        "Incoming entity_id must start with hbase= or hbase_hex=");
    mComponents = null;
    mStringEntityId = stringEntityId;
    mIsWildcarded = false;
    mLayout = layout;
  }

  /**
   * Private constructor for JsonEntityIdParser parametrized by an array of components.
   * Validate fields as necessary.
   *
   * @param components of the formatted entity id.
   * @param wildCarded if one of the components is a wildcard.
   * @param layout is the table's layout.
   */
  private JsonEntityIdParser(
      final boolean wildCarded,
      final KijiTableLayout layout,
      final Object... components) {
    Preconditions.checkNotNull(components, "Entity ID components can't be null.");
    // Wildcarded flag is only applicable for components array.
    Preconditions.checkArgument(components.length > 0, "Must have at least one component.");
    mComponents = components;
    mStringEntityId = null;
    mIsWildcarded = wildCarded;
    mLayout = layout;
  }

  /**
   * Create JsonEntityIdParser from a string input, which can be a json string or a raw hbase
   * rowKey.
   * This method is used for entity ids specified from the URL.
   *
   * @param entityId string of the row.
   * @param layout of the table in which the entity id belongs.
   *        If null, then long components may not be recognized.
   * @return a properly constructed JsonEntityIdParser.
   * @throws IOException if JsonEntityIdParser can not be properly constructed.
   */
  public static JsonEntityIdParser create(
      final String entityId,
      final KijiTableLayout layout) throws IOException {
    if (entityId.startsWith(HBASE_ROW_KEY_PREFIX)
        || entityId.startsWith(HBASE_HEX_ROW_KEY_PREFIX)) {
      return new JsonEntityIdParser(entityId, layout);
    } else {
      final JsonParser parser = new JsonFactory().createJsonParser(entityId)
          .enable(Feature.ALLOW_COMMENTS)
          .enable(Feature.ALLOW_SINGLE_QUOTES)
          .enable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
      final JsonNode node = BASIC_MAPPER.readTree(parser);
      return create(node, layout);
    }
  }

  /**
   * Create JsonEntityIdParser from entity id and layout.
   *
   * @param entityId of the row.
   * @param layout of the table containing the row.
   * @return a properly constructed JsonEntityIdParser.
   * @throws IOException if JsonEntityIdParser can not be properly constructed.
   */
  public static JsonEntityIdParser create(
      final EntityId entityId,
      final KijiTableLayout layout) throws IOException {
    final Object keysFormat = layout.getDesc().getKeysFormat();
    final RowKeyEncoding encoding = getEncoding(keysFormat);
    // Either we are dealing with a hash_prefix entity_id in which case it's not wildcarded
    // and it's a single component entity_id. If we are dealing with a formatted entity_id
    // then either the materialization of the rowkey was suppressed (in which case it's as good as
    // a hashed rowkey) or it's a normal componentized rowkey.
    switch (encoding) {
    case HASH_PREFIX:
      return new JsonEntityIdParser(
          false,
          layout,
          Bytes.toString((byte[]) entityId.getComponentByIndex(0)));
    case FORMATTED:
      final HashSpec hashSpec = ((RowKeyFormat2) keysFormat).getSalt();
      if (hashSpec.getSuppressKeyMaterialization()) {
        return new JsonEntityIdParser(
            String.format("hbase=%s", Bytes.toStringBinary(entityId.getHBaseRowKey())),
            layout);
      } else {
        return new JsonEntityIdParser(false, layout, entityId.getComponents());
      }
    default:
      // Treat all other formats as a raw/hashed key.
      return new JsonEntityIdParser(
          String.format("hbase=%s", Bytes.toStringBinary(entityId.getHBaseRowKey())),
          layout);
    }
  }

  /**
   * Gets row key encoding of a row key format.
   *
   * @param keysFormat row key format.
   * @return row key encoding.
   * @throws IOException if row key format is unrecognized.
   */
  private static RowKeyEncoding getEncoding(final Object keysFormat) throws IOException {
    if (keysFormat instanceof RowKeyFormat) {
      return ((RowKeyFormat) keysFormat).getEncoding();
    } else if (keysFormat instanceof RowKeyFormat2) {
      return ((RowKeyFormat2) keysFormat).getEncoding();
    } else {
      throw new IOException(
          String.format("Unrecognized row key format: %s", keysFormat.getClass()));
    }
  }

  /**
   * Gets the RowKeyFormat2 of the provided layout, if it exists. Otherwise, null.
   *
   * @param layout of the table to find the RowKeyFormat2.
   * @return the RowKeyFormat2, null if the layout has RowKeyFormat1.
   * @throws IOException if the keys format can not be ascertained.
   */
  private static RowKeyFormat2 getRKF2(final KijiTableLayout layout) throws IOException {
    if (null != layout
        && RowKeyEncoding.FORMATTED == getEncoding(layout.getDesc().getKeysFormat())) {
      return (RowKeyFormat2) layout.getDesc().getKeysFormat();
    } else {
      return null;
    }
  }

  /**
   * Create JsonEntityIdParser from a JSON node.
   *
   * @param node is the JSON representation of the formatted entity_id.
   * @param layout of the table in which the entity id belongs.
   *        If null, then long components may not be recognized.
   * @return a properly constructed JsonEntityIdParser.
   * @throws IOException if JsonEntityIdParser can not be properly constructed.
   */
  public static JsonEntityIdParser create(
      final JsonNode node,
      final KijiTableLayout layout) throws IOException {

    RowKeyFormat2 format = getRKF2(layout);

    if (node.isArray()) {
      final Object[] components = new Object[node.size()];
      boolean wildCarded = false;
      for (int i = 0; i < node.size(); i++) {
        final Object component = getNodeValue(node.get(i));
        if (component.equals(WildcardSingleton.INSTANCE)) {
          wildCarded = true;
          components[i] = null;
        } else if (null != format
            && ComponentType.LONG == format.getComponents().get(i).getType()) {
          components[i] = ((Number) component).longValue();
        } else {
          components[i] = component;
        }
      }
      return new JsonEntityIdParser(wildCarded, layout, components);
    } else {
      // Disallow non-arrays.
      throw new IllegalArgumentException(
          "Provide components wrapped as a JSON array or provide the row key.");
    }
  }

  /**
   * Gets the array of components.
   *
   * @return array of components.
   */
  public Object[] getComponents() {
    return mComponents;
  }

  /**
   * Are any of the components wildcarded...
   *
   * @return true iff at least one component is a wildcard (indicated by a null).
   */
  public boolean isWildcarded() {
    return mIsWildcarded;
  }

  /**
   * Gets the json node eid (which can be null if the eid was backed as a string).
   *
   * @return json node of the eid.
   */
  public JsonNode getJsonEntityId() {
    return BASIC_MAPPER.valueToTree(mComponents);
  }

  /**
   * Gets the string representation of the eid.
   *
   * @return string representation of eid.
   */
  public String getStringEntityId() {
    return mStringEntityId;
  }

  /**
   * If the eid backed by a json or a string?
   *
   * @return true iff eid is backed by json node.
   */
  public boolean hasComponents() {
    return mComponents != null;
  }

  /**
   * Construct eid from a entity id string.
   * Formatted entity ids mustn't have wildcards in order to resolve.
   *
   * @return the eid.
   * @throws IOException if construction of eid fails due to incorrect user input.
   */
  public EntityId getEntityId() throws IOException {
    if (this.hasComponents()) {
      if (this.isWildcarded()) {
        throw new IllegalArgumentException(
            "Entity id must be fully specified for resolution, i.e. without wildcards.");
      }
      return EntityIdFactory.getFactory(mLayout).getEntityId(mComponents);
    } else {
      final EntityIdFactory factory = EntityIdFactory.getFactory(mLayout);
      return factory.getEntityIdFromHBaseRowKey(parseBytes(getStringEntityId()));
    }
  }

  /**
   * Gets byte array from string entity id given in "hbase=" or "hbase_hex" format.
   *
   * @param stringEntityId representing the row to acquire byte array for.
   * @return byte array of entity id.
   * @throws IOException if the ASCII-encoded hex was improperly formed.
   */
  private static byte[] parseBytes(final String stringEntityId) throws IOException {
    if (stringEntityId.startsWith(HBASE_ROW_KEY_PREFIX)) {
      final String rowKeySubstring = stringEntityId.substring(HBASE_ROW_KEY_PREFIX.length());
      return Bytes.toBytesBinary(rowKeySubstring);
    } else if (stringEntityId.startsWith(HBASE_HEX_ROW_KEY_PREFIX)) {
      final String rowKeySubstring = stringEntityId.substring(HBASE_HEX_ROW_KEY_PREFIX.length());
      try {
        return Hex.decodeHex(rowKeySubstring.toCharArray());
      } catch (DecoderException de) {
        // Re-wrap decoder exception as IOException.
        throw new IOException(de.getMessage());
      }
    } else {
      throw new IllegalArgumentException("Passed string must be prefixed by hbase= or hbase_hex=.");
    }
  }

  /**
   * Converts a JSON string, integer, or wildcard (empty array)
   * node into a Java object (String, Integer, Long, WILDCARD, or null).
   *
   * @param node JSON string, integer numeric, or wildcard (empty array) node.
   * @return the JSON value, as a String, an Integer, a Long, a WILDCARD, or null.
   * @throws JsonParseException if the JSON node is not String, Integer, Long, WILDCARD, or null.
   */
  private static Object getNodeValue(JsonNode node) throws JsonParseException {
    // TODO: Write tests to distinguish integer and long components.
    if (node.isInt()) {
      return node.asInt();
    } else if (node.isLong()) {
      return node.asLong();
    } else if (node.isTextual()) {
      return node.asText();
    } else if (node.isArray() && node.size() == 0) {
      // An empty array token indicates a wildcard.
      return WildcardSingleton.INSTANCE;
    } else if (node.isNull()) {
      return null;
    } else {
      throw new JsonParseException(String.format(
          "Invalid JSON value: '%s', expecting string, int, long, null, or wildcard [].", node),
          null);
    }
  }

  /**
   * Singleton object to use to represent a wildcard.
   */
  private static enum WildcardSingleton {
    INSTANCE;
  }

  @Override
  public String toString() {
    if (this.hasComponents()) {
      return this.getJsonEntityId().toString();
    } else {
      return this.getStringEntityId();
    }
  }
}
