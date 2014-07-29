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

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.util.ByteArrayFormatter;
import org.kiji.schema.util.Hasher;

/**
 * Implements the Formatted Entity Id row key. This allows users to specify keys composed
 * of one or more components of the primitive types of string, integer or long with some
 * flexibility to specify hash prefixing.
 *
 * <h2>Goals</h2>
 * <ul>
 * <li>Allow logical hierarchies in the row key. E.g. “store_id:product_id”.</li>
 * <li>Simplify ordering for primitive types (especially ints and longs).</li>
 * <li>Allow range scans over part of the key.</li>
 * <li>Simplify load distribution.</li>
 * <li>Allow nullable components while preserving ordering.</li>
 * </ul>
 *
 * <h2>Specifying formatted row keys in the layout</h2>
 * In the layout file for the table:
 * <ul>
 *   <li>Use RowKeyFormat2</li>
 *   <li>Set RowKeyEncoding to FORMATTED.</li>
 *   <li>Set the hash spec to specify the hashing algorithm and hash size
 *   for the hash prefix to your key.</li>
 *   <li>Set range_scan_start_index to the element at which you would like
 *   to support range scans. Components to the left of this one will be
 *   used to calculate the hash.</li>
 *   <li>Set nullable_start_index to determine which components can be null.</li>
 *   <li>Specify the primitive types of the composite key using
 *   {@link org.kiji.schema.avro.RowKeyComponent}</li>
 * </ul>
 * <p>For more details, refer to {@link RowKeyFormat2}</p>
 *
 * <h2>Hbase encoding of formatted entity IDs</h2>
 * <ul>
 *   <li>Key begins with a hash of hash_size bytes as specified in the
 *   {@link org.kiji.schema.avro.HashSpec}</li>
 *   <li>String components are UTF-8 encoded followed by '\u0000'</li>
 *   <li>Integers and longs are in their two’s complement representation with the MSB toggled for
 *   preserving ordering.</li>
 *   <li>Integers are 4 byte long.</li>
 *   <li>Longs are 8 byte long.</li>
 *   <li>In order to preserve ordering, all components to the right of a null component must be
 *   null.</li>
 * </ul>
 *
 * <h2>Example</h2>
 * <p>
 * The json below creates a composite key with 3 components of type string, integer and long.
 * There will be a two byte hash of the first two components prefixed to the key. By default,
 * all except the first components can be set to null.
 * </p>
 *
 * <code>
 * keys_format : {
 *   encoding : "FORMATTED",
 *   salt : {
 *     hash_size : 2
 *   },
 *   range_scan_start_index: 2,
 *   components : [ {
 *     name : "dummy",
 *     type : "STRING"
 *   }, {
 *     name : "anint",
 *     type : "INTEGER"
 *   }, {
 *     name : "along",
 *     type : "LONG"
 *   } ]
 * }
 * </code>
 */
@ApiAudience.Private
public final class FormattedEntityId extends EntityId {
  // HBase row key bytes. The encoded components of the row key
  // potentially including a hash prefix, as specified in the row key format.
  private byte[] mHBaseRowKey;

  private List<Object> mComponentValues;

  private RowKeyFormat2 mRowKeyFormat;

  private static final Logger LOG = LoggerFactory.getLogger(FormattedEntityId.class);

  /**
   * Convert class of object to the correct ComponentType.
   * @param obj Input object (key component).
   * @return ComponentType representing the class of the input object.
   */
  private static ComponentType getType(Object obj) {
    if (obj instanceof String) {
      return ComponentType.STRING;
    } else if (obj instanceof Integer) {
      return ComponentType.INTEGER;
    } else if (obj instanceof Long) {
      return ComponentType.LONG;
    }
    throw new EntityIdException("Unexpected type for Component " + obj.getClass().getName());
  }

  /**
   * Creates a FormattedEntityId from the specified Kiji row key.
   *
   * @param kijiRowKey An ordered <b>mutable</b> list of objects of row key components. The
   *    contents of this list may be modified in case of any type promotions from
   *    Integer to Long.
   * @param format The RowKeyFormat2 as specified in the layout file.
   * @return a new FormattedEntityId with the specified Kiji row key.
   */
  static FormattedEntityId getEntityId(List<Object> kijiRowKey,
      RowKeyFormat2 format) {
    Preconditions.checkNotNull(format);
    Preconditions.checkNotNull(format.getSalt());
    Preconditions.checkNotNull(kijiRowKey);

    // Validity check for kiji  Row Key.
    if (kijiRowKey.size() > format.getComponents().size()) {
      throw new EntityIdException("Too many components in kiji Row Key");
    }
    if (kijiRowKey.size() < format.getComponents().size()) {
      int krk = kijiRowKey.size();
      int fgc = format.getComponents().size();
      if (LOG.isDebugEnabled()) {
        LOG.debug(
              "{} components found in {}, {} expected by row key format."
              + "The last {} components implicitly set to null",
              krk, kijiRowKey.toString(), fgc, fgc - krk);
      }
    }
    if (kijiRowKey.size() < format.getNullableStartIndex()) {
      throw new EntityIdException("Too few components in kiji Row key");
    }

    // Validate the components passed in against the row key format such
    // as prevent non-null component from following null components and checking
    // component types against the format specified.
    boolean hasSeenNull = false;
    for (int i = 0; i < kijiRowKey.size(); i++) {
      // Prevent non-null components that follow null components
      if (hasSeenNull) {
        if (null == kijiRowKey.get(i)) {
          continue;
        } else {
          throw new EntityIdException("Non null component follows null component");
        }
      } else if (null == kijiRowKey.get(i)) {
        // we found a null, check if this is at a position greater than or equal to
        // nullable_start_index (the position from which null values are allowed)
        // also set the flag indicating we've seen a null value
        if (format.getNullableStartIndex() <= i) {
          hasSeenNull = true;
          continue;
        } else {
          throw new EntityIdException("Unexpected null component in kiji row key."
              + String.format("Expected at least %d non-null components",
                  format.getNullableStartIndex()));
        }
      } else {
        // for non-null components ensure that the type matches the format spec
        ComponentType type = getType(kijiRowKey.get(i));
        if (null == type || type != format.getComponents().get(i).getType()) {
          if (type == ComponentType.INTEGER
               && format.getComponents().get(i).getType() == ComponentType.LONG) {
            kijiRowKey.set(i, ((Integer) kijiRowKey.get(i)).longValue());
          } else {
          throw new EntityIdException(String.format(
              "Invalid type for component %s at index %d in kijiRowKey",
              kijiRowKey.get(i).toString(), i));
          }
        }
      }
    }

    byte[] hbaseRowKey = null;

    hbaseRowKey = makeHbaseRowKey(format, kijiRowKey);

    return new FormattedEntityId(format, hbaseRowKey, kijiRowKey);
  }

  /**
   * Creates a FormattedEntityId from the specified HBase row key.
   *
   * @param hbaseRowKey A byte[] containing the HBase row key.
   * @param format The RowKeyFormat as specified in the layout file.
   * @return a new FormattedEntityId with the specified HBase row key.
   */
  static FormattedEntityId fromHBaseRowKey(byte[] hbaseRowKey, RowKeyFormat2 format) {
    Preconditions.checkNotNull(format);
    Preconditions.checkNotNull(format.getSalt());
    Preconditions.checkNotNull(hbaseRowKey);
    // we modify the hbaseRowKey in the makeKijiRowKey code for integer encoding, so we make
    // a copy of it to pass to makeKijiRowKey:
    byte[] cloneHbaseKey = hbaseRowKey.clone();
    List<Object> kijiRowKey = makeKijiRowKey(format, cloneHbaseKey);
    return new FormattedEntityId(format, hbaseRowKey, kijiRowKey);
  }

  /**
   * Create an hbase row key, which is a byte array from the given formatted kijiRowKey.
   * This method requires that the kijiRowKey argument is the correct length for the specified
   * format.
   * The following encoding will be used to ensure correct ordering:
   * Strings are UTF-8 encoded and terminated by a null byte. Strings cannot contain "\u0000".
   * Integers are exactly 4 bytes long.
   * Longs are exactly 8 bytes long.
   * Both integers and longs have the sign bit flipped so that their values are wrapped around to
   * create the correct lexicographic ordering. (i.e. after converting to byte array,
   * MIN_INT < 0 < MAX_INT).
   * Hashed components are exactly hash_size bytes long and are the first component of
   * the hbase key.
   * Except for the first, all components of a kijiRowKey can be null. However, to maintain
   * ordering, all components to the right of a null component must also be null. Nullable index
   * in the row key format specifies which component (and hence following components) are nullable.
   * By default, the hash only uses the first component, but this can be changed using the Range
   * Scan index.
   *
   * @param format The formatted row key format for this table.
   * @param kijiRowKey An ordered list of Objects of the key components.
   * @return A byte array representing the encoded Hbase row key.
   */
  private static byte[] makeHbaseRowKey(RowKeyFormat2 format, List<Object> kijiRowKey) {

    ArrayList<byte[]> hbaseKey = new ArrayList<byte[]>();
    final byte zeroDelim = 0;

    int pos;
    for (pos = 0; pos < kijiRowKey.size(); pos++) {
      // we have already done the validation check for null cascades.
      if (null == kijiRowKey.get(pos)) {
        continue;
      }
      byte[] tempBytes;
      switch (getType(kijiRowKey.get(pos))) {
        case STRING:
          if (((String)kijiRowKey.get(pos)).contains("\u0000")) {
            throw new EntityIdException("String component cannot contain \u0000");
          }
          try {
            hbaseKey.add(((String) kijiRowKey.get(pos)).getBytes("UTF-8"));
          } catch (UnsupportedEncodingException e) {
            LOG.error(e.toString());
            throw new EntityIdException(String.format(
                "UnsupportedEncoding for component %d", pos));
          }
          break;
        case INTEGER:
          int temp = (Integer) kijiRowKey.get(pos);
          tempBytes = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE)
              .putInt(temp).array();
          tempBytes[0] = (byte)((int) tempBytes[0] ^ (int)Byte.MIN_VALUE);
          hbaseKey.add(tempBytes);
          break;
        case LONG:
          long templong = (Long) kijiRowKey.get(pos);
          tempBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(templong).array();
          tempBytes[0] = (byte)((int) tempBytes[0] ^ (int)Byte.MIN_VALUE);
          hbaseKey.add(tempBytes);
          break;
        default:
          throw new RuntimeException("Invalid code path");
      }
    }

    // hash stuff
    int hashUpto = format.getRangeScanStartIndex() - 1;
    ByteArrayOutputStream tohash = new ByteArrayOutputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (pos = 0; pos <= hashUpto && pos < hbaseKey.size(); pos++) {
      tohash.write(hbaseKey.get(pos), 0, hbaseKey.get(pos).length);
    }
    byte[] hashed = Arrays.copyOfRange(Hasher.hash(tohash.toByteArray()), 0,
        format.getSalt().getHashSize());
    baos.write(hashed, 0, hashed.length);

    // to materialize or not to materialize that is the question
    if (format.getSalt().getSuppressKeyMaterialization()) {
      return baos.toByteArray();
    } else {
      for (pos = 0; pos < hbaseKey.size(); pos++) {
        baos.write(hbaseKey.get(pos), 0, hbaseKey.get(pos).length);
        if (format.getComponents().get(pos).getType() == ComponentType.STRING
            || format.getComponents().get(pos) == null) {
          // empty strings will be encoded as null, hence we need to delimit them too
          baos.write(zeroDelim);
        }
      }
      return baos.toByteArray();
    }
  }

  @Override
  public byte[] getHBaseRowKey() {
    return mHBaseRowKey;
  }

  /**
   * Decode a byte array containing an hbase row key into an ordered list corresponding to
   * the key format in the layout file.
   *
   * @param format The row key format as specified in the layout file.
   * @param hbaseRowKey A byte array containing the hbase row key.
   * @return An ordered list of component values in the key.
   */
  private static List<Object> makeKijiRowKey(RowKeyFormat2 format, byte[] hbaseRowKey) {
    if (hbaseRowKey.length == 0) {
      throw new EntityIdException("Invalid hbase row key");
    }
    List<Object> kijiRowKey = new ArrayList<Object>();
    // skip over the hash
    int pos = format.getSalt().getHashSize();
    // we are suppressing materialization, so the components cannot be retrieved.
    int kijiRowElem = 0;
    if (format.getSalt().getSuppressKeyMaterialization()) {
      if (pos < hbaseRowKey.length) {
        throw new EntityIdException("Extra bytes in key after hash when materialization is"
            + "suppressed");
      }
      return null;
    }
    ByteBuffer buf;

    while (kijiRowElem < format.getComponents().size() && pos < hbaseRowKey.length) {
      switch (format.getComponents().get(kijiRowElem).getType()) {
        case STRING:
          // Read the row key until we encounter a Null (0) byte or end.
          int endpos = pos;
          while (endpos < hbaseRowKey.length && (hbaseRowKey[endpos] != (byte) 0)) {
            endpos += 1;
          }
          String str = null;
          try {
            str = new String(hbaseRowKey, pos, endpos - pos, "UTF-8");
          } catch (UnsupportedEncodingException e) {
            LOG.error(e.toString());
            throw new EntityIdException(String.format(
                "UnsupportedEncoding for component %d", kijiRowElem));
          }
          kijiRowKey.add(str);
          pos = endpos + 1;
          break;
        case INTEGER:
          // Toggle highest order bit to return to original 2's complement.
          hbaseRowKey[pos] =  (byte)((int) hbaseRowKey[pos] ^ (int) Byte.MIN_VALUE);
          try {
            buf = ByteBuffer.wrap(hbaseRowKey, pos, Integer.SIZE / Byte.SIZE);
          } catch (IndexOutOfBoundsException e) {
            throw new EntityIdException("Malformed hbase Row Key");
          }
          kijiRowKey.add(Integer.valueOf(buf.getInt()));
          pos = pos + Integer.SIZE / Byte.SIZE;
          break;
        case LONG:
          // Toggle highest order bit to return to original 2's complement.
          hbaseRowKey[pos] =  (byte)((int) hbaseRowKey[pos] ^ (int) Byte.MIN_VALUE);
          try {
            buf = ByteBuffer.wrap(hbaseRowKey, pos, Long.SIZE / Byte.SIZE);
          } catch (IndexOutOfBoundsException e) {
            throw new EntityIdException("Malformed hbase Row Key");
          }
          kijiRowKey.add(Long.valueOf(buf.getLong()));
          pos = pos + Long.SIZE / Byte.SIZE;
          break;
        default:
          throw new RuntimeException("Invalid code path");
      }
      kijiRowElem += 1;
    }

    // Fail if there are extra bytes in hbase row key.
    if (pos < hbaseRowKey.length) {
      throw new EntityIdException("Extra bytes in hbase row key cannot be mapped to any "
          + "component");
    }

    // Fail if we encounter nulls before it is legal to do so.
    if (kijiRowElem < format.getNullableStartIndex()) {
      throw new EntityIdException("Too few components decoded from hbase row key. Component "
          + "number " + kijiRowElem + " cannot be null");
    }

    // finish up with nulls for everything that wasn't in the key
    for (; kijiRowElem < format.getComponents().size(); kijiRowElem++) {
      kijiRowKey.add(null);
    }

    return kijiRowKey;
  }

  /**
   * Creates a new FormattedEntityId.
   * @param format Format of the row key as specified in the layout file.
   * @param hbaseRowKey Byte array containing the hbase row key.
   * @param kijiRowKey An ordered list of row key components.
   */
  private FormattedEntityId(RowKeyFormat2 format, byte[] hbaseRowKey, List<Object> kijiRowKey) {
    mRowKeyFormat = Preconditions.checkNotNull(format);
    Preconditions.checkArgument(format.getEncoding() == RowKeyEncoding.FORMATTED);
    Preconditions.checkNotNull(format.getSalt(),
        "Formatted entityIds may not specify a null 'salt' field in RowKeyFormat2.");
    mHBaseRowKey = hbaseRowKey;
    if (format.getSalt().getSuppressKeyMaterialization()) {
      mComponentValues = null;
    } else {
      mComponentValues = kijiRowKey;
    }
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T getComponentByIndex(int idx) {
    Preconditions.checkState(!mRowKeyFormat.getSalt().getSuppressKeyMaterialization(),
        "Cannot retrieve components as materialization is suppressed");
    Preconditions.checkArgument(idx >= 0 && idx < mComponentValues.size());
    return (T) mComponentValues.get(idx);
  }

  /** {@inheritDoc} */
  @Override
  public List<Object> getComponents() {
    Preconditions.checkState(!mRowKeyFormat.getSalt().getSuppressKeyMaterialization(),
        "Cannot retrieve components as materialization is suppressed");
    return Collections.unmodifiableList(mComponentValues);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    if (!mRowKeyFormat.getSalt().getSuppressKeyMaterialization()) {
      return Objects.toStringHelper(FormattedEntityId.class)
          .add("components", Joiner.on(",").join(mComponentValues))
          .add("hbase", Bytes.toStringBinary(mHBaseRowKey))
          .toString();
    } else {
      return Objects.toStringHelper(FormattedEntityId.class)
          .addValue("components are suppressed")
          .add("hbase", Bytes.toStringBinary(mHBaseRowKey))
          .toString();
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toShellString() {
    if (mRowKeyFormat.getSalt().getSuppressKeyMaterialization()) {
      return String.format("hbase=hex:%s", ByteArrayFormatter.toHex(mHBaseRowKey));
    }

    /** Set of characters which must be escaped */
    HashSet<Character> escapeSet = Sets.newHashSet('"', '\\', '\'');
    ArrayList<String> componentStrings = Lists.newArrayList();
    for (Object component : mComponentValues) {
      if (component == null) {
        componentStrings.add("null");
      } else {
        String unescapedString = component.toString();
        ArrayList<Character> escapedString = Lists.newArrayList();
        for (char c: unescapedString.toCharArray()) {
          if (escapeSet.contains(c)) {
            escapedString.add('\\');
          }
          escapedString.add(c);
        }
        if (getType(component) == ComponentType.STRING) {
          componentStrings.add(String.format("'%s'", StringUtils.join(escapedString, "")));
        } else {
          componentStrings.add(component.toString());
        }
      }
    }
    return "[" + StringUtils.join(componentStrings, ", ") + "]";
  }
}
