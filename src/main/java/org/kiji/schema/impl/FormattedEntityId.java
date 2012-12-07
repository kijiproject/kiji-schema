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

package org.kiji.schema.impl;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Preconditions;
import org.kiji.schema.EntityId;
import org.kiji.schema.avro.*;
import org.kiji.schema.util.Hasher;

/**
 * Implements the Formatted Entity Id row key. This allows users to specify keys composed
 * of either the primitive types of string, integer or long or a fixed size hash of one of
 * the other components.
 */
public class FormattedEntityId extends EntityId {
  private final RowKeyFormat mFormat;

  /** HBase row key bytes. */
  private byte[] mHBaseRowKey;

  private List<Object> mComponentValues;

  private static ComponentType getType(Object obj) {
    ComponentType type = null;
    if (obj instanceof String) {
      type = ComponentType.STRING;
    } else if (obj instanceof Integer) {
      type = ComponentType.INTEGER;
    } else if (obj instanceof Long) {
      type = ComponentType.LONG;
    }
    return type;
  }

  /**
   * Creates a FormattedEntityId from the specified Kiji row key.
   *
   * @param kijiRowKey
   * @param format The RowKeyFormat as specified in the layout file.
   * @return a new FormattedEntityId with the specified Kiji row key.
   */
  public static FormattedEntityId fromKijiRowKey(List<Object> kijiRowKey,
      RowKeyFormat format) {

    Preconditions.checkNotNull(kijiRowKey);
    // Validity check for kiji  Row Key.
    if (kijiRowKey.size() > format.getComponents().size()) {
      throw new EntityIdException("Too many components in kiji Row Key");
    }
    if (kijiRowKey.size() < format.getNullableIndex()) {
      throw new EntityIdException("Too few components in kiji Row key");
    }

    boolean flag_null = false;
    for (int i = 0; i < kijiRowKey.size(); i++) {
      if (flag_null) {
        if (null == kijiRowKey.get(i)) {
          continue;
        } else {
          throw new EntityIdException("Non null component follows null component");
        }
      }
      if (null == kijiRowKey.get(i)) {
        if (format.getNullableIndex() <= i) {
          flag_null = true;
          continue;
        } else {
          throw new EntityIdException("Unexpected null component in kiji row key");
        }
      } else {
        ComponentType type = getType(kijiRowKey.get(i));
        if ((null == type) || type != format.getComponents().get(i).getType()) {
          throw new EntityIdException(String.format(
              "Invalid type for component %d in kijiRowKey", i));
        }
      }
    }

    byte[] hbaserowkey = null;

    hbaserowkey = makeHbaseRowKey(format, kijiRowKey);

    return new FormattedEntityId(format, hbaserowkey, kijiRowKey);
  }

  /**
   * Creates a FormattedEntityId from the specified HBase row key.
   *
   * @param hbaseRowKey A byte[] containing the HBase row key.
   * @param format The RowKeyFormat as specified in the layout file.
   * @return a new FormattedEntityId with the specified HBase row key.
   */
  public static FormattedEntityId fromHBaseRowKey(byte[] hbaseRowKey, RowKeyFormat format) {
    List<Object> kijiRowKey = null;
    byte[] originalKey = hbaseRowKey.clone();
    kijiRowKey = makeKijiRowKey(format, originalKey);
    return new FormattedEntityId(format, hbaseRowKey, kijiRowKey);
  }

  /**
   * Create an hbase row key, which is a byte array from the given formatted kijiRowKey.
   * The following encoding will be used to ensure correct ordering:
   * Strings are UTF-8 encoded and terminated by a null byte.
   * Integers are exactly 4 bytes long.
   * Hashed components are exactly hash_size bytes long.
   * @param format The formatted row key format for this table.
   * @param kijiRowKey An ordered list of Objects representing the key components.
   * @return A byte array representing the encoded Hbase row key.
   */
  private static byte[] makeHbaseRowKey(RowKeyFormat format, List<Object> kijiRowKey) {

    ArrayList<byte[]> hbaseKey = new ArrayList<byte[]>();
    final byte zeroDelim = 0;

    int pos;
    for (pos = 0; pos < kijiRowKey.size(); pos++) {
      // we have already done the validation check for null cascades.
      if (null == kijiRowKey.get(pos)) {
        continue;
      }
      switch (getType(kijiRowKey.get(pos))) {
        case STRING:
          if (((String)kijiRowKey.get(pos)).contains("\u0000")) {
            throw new EntityIdException("String component cannot contain \u0000");
          }
          try {
            hbaseKey.add(((String)kijiRowKey.get(pos)).getBytes("UTF-8"));
          } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new EntityIdException(String.format(
                "UnsupportedEncoding for component %d", pos));
          }
          break;
        case INTEGER:
          int temp = (Integer)kijiRowKey.get(pos);
          byte[] tempBytes = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE)
              .putInt(temp).array();
          tempBytes[0] = (byte)((int)tempBytes[0] ^ (int)Byte.MIN_VALUE);
          hbaseKey.add(tempBytes);
          break;
        case LONG:
          long templong = (Long)kijiRowKey.get(pos);
          tempBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(templong).array();
          tempBytes[0] = (byte)((int)tempBytes[0] ^ (int)Byte.MIN_VALUE);
          hbaseKey.add(tempBytes);
          break;
        default:
          throw new RuntimeException("Invalid code path");
      }
    }

    // hash stuff
    int hash_upto = format.getRangeScanIndex() - 1;
    ByteArrayOutputStream tohash = new ByteArrayOutputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (pos = 0; pos <= hash_upto; pos++) {
      tohash.write(hbaseKey.get(pos), 0, hbaseKey.get(pos).length);
    }
    byte[] hashed = Arrays.copyOfRange(Hasher.hash(tohash.toByteArray()), 0,
        format.getSalt().getHashSize());
    baos.write(hashed, 0, hashed.length);

    // to materialize or not to materialize that is the question
    if (format.getSalt().getSuppressKeyMaterialization()) {
      pos = format.getRangeScanIndex();
    } else {
      pos = 0;
    }
    for (; pos < hbaseKey.size(); pos++) {
      baos.write(hbaseKey.get(pos), 0, hbaseKey.get(pos).length);
      if (format.getComponents().get(pos).getType() == ComponentType.STRING) {
        baos.write(zeroDelim);
      }
    }

    return baos.toByteArray();
  }

  @Override
  public byte[] getHBaseRowKey() {
    return mHBaseRowKey;
  }

  @Override
  public List<Object> getKijiRowKey() {
    return mComponentValues;
  }

  /**
   * Convert a byte array containing an hbase row key into a Map corresponding to the key_spec
   * in the layout file.
   * @param format The row key format as specified in the layout file.
   * @param hbaseRowKey A byte array containing the hbase row key.
   * @return An ordered list of component values in the key.
   * @throws UnsupportedEncodingException If the string component cannot be converted to UTF-8.
   */
  private static List<Object> makeKijiRowKey(RowKeyFormat format, byte[] hbaseRowKey) {
    if (hbaseRowKey.length == 0) {
      throw new EntityIdException("Invalid hbase row key");
    }
    List<Object> kijiRowKey = new ArrayList<Object>();
    // skip over the hash
    int pos = format.getSalt().getHashSize();
    int kijiRowElem = 0;
    ByteBuffer buf;

    while (kijiRowElem < format.getComponents().size() && pos < hbaseRowKey.length) {
      switch (format.getComponents().get(kijiRowElem).getType()) {
        case STRING:
          // Read the row key until we encounter a Null (0) byte or end.
          int endpos = pos;
          while (endpos < hbaseRowKey.length && (hbaseRowKey[endpos] != (byte)0)) {
            endpos += 1;
          }
          String str = null;
          try {
            str = new String(hbaseRowKey, pos, endpos - pos, "UTF-8");
          } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new EntityIdException(String.format(
                "UnsupportedEncoding for component %d", kijiRowElem));
          }
          kijiRowKey.add(str);
          pos = endpos + 1;
          break;
        case INTEGER:
          // Toggle highest order bit to return to original 2's complement.
          hbaseRowKey[pos] =  (byte)((int)hbaseRowKey[pos] ^ (int)Byte.MIN_VALUE);
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
          hbaseRowKey[pos] =  (byte)((int)hbaseRowKey[pos] ^ (int)Byte.MIN_VALUE);
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

    // finish up with nulls for everything that wasn't in the key/not materialized.
    for (; kijiRowElem < format.getComponents().size(); kijiRowElem++) {
      kijiRowKey.add(null);
    }

    return kijiRowKey;
  }

  /**
   * Creates a new FormattedEntityId.
   * @param format Format of the row key as specified in the layout file.
   * @param hbaseRowKey Byte array containing the hbase row key.
   * @param kijiRowKey Map of the kiji row key components to values.
   */
  public FormattedEntityId(RowKeyFormat format, byte[] hbaseRowKey, List<Object> kijiRowKey) {
    mFormat = format;
    mHBaseRowKey = hbaseRowKey;
    mComponentValues = kijiRowKey;
  }

  /** {@inheritDoc} */
  @Override
  public RowKeyFormat getFormat() {
    return mFormat;
  }

}
