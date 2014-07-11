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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.platform.SchemaPlatformBridge;
import org.kiji.schema.util.FromJson;
import org.kiji.schema.util.Hasher;
import org.kiji.schema.util.ToJson;

/**
 * A KijiRowFilter that filters out rows that do not match the components
 * specified.  Only components that are specified are matched on, and the
 * components that are left {@code null} can match anything.  Think of this as
 * a bit mask for components.  This filter understands hash prefixes and ignores
 * them when attempting to match.
 *
 * <p>For example, for a table defined with a row key format of {@code INTEGER},
 * {@code LONG}, {@code STRING}:</p>
 * <table>
 * <tr><th>Filter</th><th>Row Key</th><th>Match</th></tr>
 * <tr>
 *   <td>({@code 100, 1000L, "value"})</td>
 *   <td>({@code 100, 1000L, "value"})</td>
 *   <td>yes</td>
 * </tr>
 * <tr>
 *   <td>({@code 100, 1000L, "value"})</td>
 *   <td>({@code 200, 2000L, "no value"})</td>
 *   <td>no</td>
 * </tr>
 * <tr>
 *   <td>({@code 100, null, "value"})</td>
 *   <td>({@code 100, 2000L, "value"})</td>
 *   <td>yes</td>
 * </tr>
 * <tr>
 *   <td>({@code 100, null, "value"})</td>
 *   <td>({@code 100, null, null})</td>
 *   <td>no</td>
 * </tr>
 * </table>
 *
 * <p>The filter functionality is accomplished using a {@link
 * org.apache.hadoop.hbase.filter.RegexStringComparator} in combination with a {@link RowFilter}.
 * The regular expression is constructed using the same component conversion rules used to
 * create a FormattedEntityId.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class FormattedEntityIdRowFilter extends KijiRowFilter {
  /** The name of the row key format node. */
  private static final String ROW_KEY_FORMAT_NODE = "rowKeyFormat";

  /** The name of the components node. */
  private static final String COMPONENTS_NODE = "components";

  /** The format of the row key. */
  private final RowKeyFormat2 mRowKeyFormat;

  /**
   * The list of components to match on, with {@code null}s indicating a match
   * on any value.
   */
  private final Object[] mComponents;

  /**
   * Creates a new <code>FormattedEntityIdRowFilter</code> instance. The row key
   * format must have an encoding of {@link RowKeyEncoding#FORMATTED}, and if
   * there is a salt defined, it must not be set to suppress key
   * materialization.  If key materialization were suppressed, then there would
   * be no component fields to match against.
   *
   * @param rowKeyFormat the row key format of the table this filter will be used on
   * @param components the components to match on, with {@code null}s indicating
   *     a match on any value. If fewer than the number of components defined in
   *     the row key format are supplied, the rest will be filled in as {@code
   *     null}.
   */
  public FormattedEntityIdRowFilter(RowKeyFormat2 rowKeyFormat, Object... components) {
    Preconditions.checkNotNull(rowKeyFormat, "RowKeyFormat must be specified");
    Preconditions.checkArgument(rowKeyFormat.getEncoding().equals(RowKeyEncoding.FORMATTED),
        "FormattedEntityIdRowFilter only works with formatted entity IDs");
    if (null != rowKeyFormat.getSalt()) {
      Preconditions.checkArgument(!rowKeyFormat.getSalt().getSuppressKeyMaterialization(),
          "FormattedEntityIdRowFilter only works with materialized keys");
    }
    Preconditions.checkNotNull(components, "At least one component must be specified");
    Preconditions.checkArgument(components.length > 0, "At least one component must be specified");
    Preconditions.checkArgument(components.length <= rowKeyFormat.getComponents().size(),
        "More components given (%s) than are in the row key format specification (%s)",
        components.length, rowKeyFormat.getComponents().size());

    mRowKeyFormat = rowKeyFormat;

    // Fill in 'null' for any components defined in the row key format that have
    // not been passed in as components for the filter.  This effectively makes
    // a prefix filter, looking at only the components defined at the beginning
    // of the EntityId.
    if (components.length < rowKeyFormat.getComponents().size()) {
      mComponents = new Object[rowKeyFormat.getComponents().size()];
      for (int i = 0; i < components.length; i++) {
        mComponents[i] = components[i];
      }
    } else {
      mComponents = components;
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return KijiDataRequest.builder().build();
  }

  /** {@inheritDoc} */
  @Override
  public Filter toHBaseFilter(Context context) throws IOException {
    // In the case that we have enough information to generate a prefix, we will
    // construct a PrefixFilter that is AND'ed with the RowFilter.  This way,
    // when the scan passes the end of the prefix, it can end the filtering
    // process quickly.
    boolean addPrefixFilter = false;
    ByteArrayOutputStream prefixBytes = new ByteArrayOutputStream();

    // Define a regular expression that effectively creates a mask for the row
    // key based on the key format and the components passed in.  Prefix hashes
    // are skipped over, and null components match any value.
    // ISO-8859-1 is used so that numerals map to valid characters
    // see http://stackoverflow.com/a/1387184

    // Use the embedded flag (?s) to turn on single-line mode; this makes the
    // '.' match on line terminators.  The RegexStringComparator uses the DOTALL
    // flag during construction, but it does not use that flag upon
    // deserialization.  This bug has been fixed in HBASE-5667, part of the 0.95
    // release.
    StringBuilder regex = new StringBuilder("(?s)^"); // ^ matches the beginning of the string
    if (null != mRowKeyFormat.getSalt()) {
      if (mRowKeyFormat.getSalt().getHashSize() > 0) {
        // If all of the components included in the hash have been specified,
        // then match on the value of the hash
        Object[] prefixComponents =
            getNonNullPrefixComponents(mComponents, mRowKeyFormat.getRangeScanStartIndex());
        if (prefixComponents.length == mRowKeyFormat.getRangeScanStartIndex()) {
          ByteArrayOutputStream tohash = new ByteArrayOutputStream();
          for (Object component : prefixComponents) {
            byte[] componentBytes = toBytes(component);
            tohash.write(componentBytes, 0, componentBytes.length);
          }
          byte[] hashed = Arrays.copyOfRange(Hasher.hash(tohash.toByteArray()), 0,
              mRowKeyFormat.getSalt().getHashSize());
          for (byte hashedByte : hashed) {
            regex.append(String.format("\\x%02x", hashedByte & 0xFF));
          }

          addPrefixFilter = true;
          // Write the hashed bytes to the prefix buffer
          prefixBytes.write(hashed, 0, hashed.length);
        } else {
          // match any character exactly 'hash size' number of times
          regex.append(".{").append(mRowKeyFormat.getSalt().getHashSize()).append("}");
        }
      }
    }
    // Only add to the prefix buffer until we hit a null component.  We do this
    // here, because the prefix is going to expect to have 0x00 component
    // terminators, which we put in during this loop but not in the loop above.
    boolean hitNullComponent = false;
    for (int i = 0; i < mComponents.length; i++) {
      final Object component = mComponents[i];
      switch (mRowKeyFormat.getComponents().get(i).getType()) {
        case INTEGER:
          if (null == component) {
            // null integers can be excluded entirely if there are just nulls at
            // the end of the EntityId, otherwise, match the correct number of
            // bytes
            regex.append("(.{").append(Bytes.SIZEOF_INT).append("})?");
            hitNullComponent = true;
          } else {
            byte[] tempBytes = toBytes((Integer) component);
            // match each byte in the integer using a regex hex sequence
            for (byte tempByte : tempBytes) {
              regex.append(String.format("\\x%02x", tempByte & 0xFF));
            }
            if (!hitNullComponent) {
              prefixBytes.write(tempBytes, 0, tempBytes.length);
            }
          }
          break;
        case LONG:
          if (null == component) {
            // null longs can be excluded entirely if there are just nulls at
            // the end of the EntityId, otherwise, match the correct number of
            // bytes
            regex.append("(.{").append(Bytes.SIZEOF_LONG).append("})?");
            hitNullComponent = true;
          } else {
            byte[] tempBytes = toBytes((Long) component);
            // match each byte in the long using a regex hex sequence
            for (byte tempByte : tempBytes) {
              regex.append(String.format("\\x%02x", tempByte & 0xFF));
            }
            if (!hitNullComponent) {
              prefixBytes.write(tempBytes, 0, tempBytes.length);
            }
          }
          break;
        case STRING:
          if (null == component) {
            // match any non-zero character at least once, followed by a zero
            // delimiter, or match nothing at all in case the component was at
            // the end of the EntityId and skipped entirely
            regex.append("([^\\x00]+\\x00)?");
            hitNullComponent = true;
          } else {
            // FormattedEntityId converts a string component to UTF-8 bytes to
            // create the HBase key.  RegexStringComparator will convert the
            // value it sees (ie, the HBase key) to a string using ISO-8859-1.
            // Therefore, we need to write ISO-8859-1 characters to our regex so
            // that the comparison happens correctly.
            byte[] utfBytes = toBytes((String) component);
            String isoString = new String(utfBytes, Charsets.ISO_8859_1);
            regex.append(isoString).append("\\x00");
            if (!hitNullComponent) {
              prefixBytes.write(utfBytes, 0, utfBytes.length);
              prefixBytes.write((byte) 0);
            }
          }
          break;
        default:
          throw new IllegalStateException("Unknown component type: "
              + mRowKeyFormat.getComponents().get(i).getType());
      }
    }
    regex.append("$"); // $ matches the end of the string

    final RowFilter regexRowFilter =
        SchemaPlatformBridge.get().createRowFilterFromRegex(CompareOp.EQUAL, regex.toString());
    if (addPrefixFilter) {
      return new FilterList(new PrefixFilter(prefixBytes.toByteArray()), regexRowFilter);
    }
    return regexRowFilter;
  }

  /**
   * Return the first non-null components up to a total of {@code
   * rangeScanStartIndex}.
   *
   * @param components The full list of components
   * @param rangeScanStartIndex The index at which to stop looking for non-null
   *     components
   * @return The first non-null components up to {@code rangeScanStartIndex}
   */
  private static Object[] getNonNullPrefixComponents(Object[] components, int rangeScanStartIndex) {
    final List<Object> prefixComponents = Lists.newArrayList();
    // Create a list of the prefix set of non-null components up to the
    // specified amount.
    for (int i = 0; i < rangeScanStartIndex; i++) {
      if (components[i] != null) {
        prefixComponents.add(components[i]);
      } else {
        break;
      }
    }
    return prefixComponents.toArray();
  }

  /**
   * Convert a component to a byte array according to the rules in {@link
   * FormattedEntityId}.
   *
   * @param component The component to convert
   * @return The byte representation
   */
  private static byte[] toBytes(Object component) {
    if (component instanceof String) {
      return toBytes((String) component);
    }
    if (component instanceof Integer) {
      return toBytes((Integer) component);
    }
    if (component instanceof Long) {
      return toBytes((Long) component);
    }
    throw new IllegalArgumentException("Unknown component type: "
        + component.getClass() + "; value: " + component);
  }

  /**
   * Convert a component to a byte array according to the rules in {@link
   * FormattedEntityId}.
   *
   * @param component The component to convert
   * @return The byte representation
   */
  private static byte[] toBytes(String component) {
    return component.getBytes(Charsets.UTF_8);
  }

  /**
   * Convert a component to a byte array according to the rules in {@link
   * FormattedEntityId}.
   *
   * @param component The component to convert
   * @return The byte representation
   */
  private static byte[] toBytes(Integer component) {
    byte[] tempBytes = ByteBuffer.allocate(Bytes.SIZEOF_INT).putInt(component).array();
    // FormattedEntityId flips the sign of the most significant bit to
    // maintain byte ordering, so do the same operation here
    tempBytes[0] = (byte) ((int) tempBytes[0] ^ (int) Byte.MIN_VALUE);
    return tempBytes;
  }

  /**
   * Convert a component to a byte array according to the rules in {@link
   * FormattedEntityId}.
   *
   * @param component The component to convert
   * @return The byte representation
   */
  private static byte[] toBytes(Long component) {
    byte[] tempBytes = ByteBuffer.allocate(Bytes.SIZEOF_LONG).putLong(component).array();
    // FormattedEntityId flips the sign of the most significant bit to
    // maintain byte ordering, so do the same operation here
    tempBytes[0] = (byte) ((int) tempBytes[0] ^ (int) Byte.MIN_VALUE);
    return tempBytes;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FormattedEntityIdRowFilter)) {
      return false;
    } else {
      final FormattedEntityIdRowFilter otherFilter = (FormattedEntityIdRowFilter) other;
      return Objects.equal(otherFilter.mRowKeyFormat, this.mRowKeyFormat)
          && Arrays.equals(otherFilter.mComponents, this.mComponents);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mRowKeyFormat, mComponents);
  }

  /** {@inheritDoc} */
  @Override
  protected JsonNode toJsonNode() {
    final ObjectNode root = JsonNodeFactory.instance.objectNode();
    try {
      root.put(ROW_KEY_FORMAT_NODE,
          ToJson.toAvroJsonString(mRowKeyFormat, mRowKeyFormat.getSchema()));
      final ArrayNode components = root.putArray(COMPONENTS_NODE);
      for (int i = 0; i < mComponents.length; i++) {
        final Object component = mComponents[i];
        if (null == component) {
          components.add(components.nullNode());
        } else {
          switch (mRowKeyFormat.getComponents().get(i).getType()) {
            case INTEGER:
              components.add((Integer)component);
              break;
            case LONG:
              components.add((Long)component);
              break;
            case STRING:
              components.add((String)component);
              break;
            default:
              throw new IllegalStateException("Unknown component type: "
                  + mRowKeyFormat.getComponents().get(i).getType());
          }
        }
      }
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
    return root;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends KijiRowFilterDeserializer> getDeserializerClass() {
    return FormattedEntityIdRowFilterDeserializer.class;
  }

  /** Deserializes {@code FormattedEntityIdRowFilter}. */
  public static final class FormattedEntityIdRowFilterDeserializer
      implements KijiRowFilterDeserializer {
    /** {@inheritDoc} */
    @Override
    public KijiRowFilter createFromJson(JsonNode root) {
      try {
        final RowKeyFormat2 rowKeyFormat = (RowKeyFormat2) FromJson.fromAvroJsonString(
            root.path(ROW_KEY_FORMAT_NODE).getTextValue(), RowKeyFormat2.SCHEMA$);
        final JsonNode componentsJsonNode = root.path(COMPONENTS_NODE);
        Preconditions.checkArgument(componentsJsonNode.isArray(),
            "Node 'components' must be an array");
        final ArrayNode componentsNode = (ArrayNode)componentsJsonNode;
        final Object[] components = new Object[componentsNode.size()];
        for (int i = 0; i < components.length; i++) {
          JsonNode componentNode = componentsNode.get(i);
          if (componentNode.isNull()) {
            components[i] = null; // an explicit no-op
          } else {
            switch (rowKeyFormat.getComponents().get(i).getType()) {
              case INTEGER:
                components[i] = Integer.valueOf(componentNode.getIntValue());
                break;
              case LONG:
                components[i] = Long.valueOf(componentNode.getLongValue());
                break;
              case STRING:
                components[i] = componentNode.getTextValue();
                break;
              default:
                throw new IllegalStateException("Unknown component type: "
                    + rowKeyFormat.getComponents().get(i).getType());
            }
          }
        }
        return new FormattedEntityIdRowFilter(rowKeyFormat, components);
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }
    }
  }
}
