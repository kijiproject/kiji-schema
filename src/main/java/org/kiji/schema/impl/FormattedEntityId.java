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

  private Map<String, Object> mKeyComponentMap;

  /**
   * Creates a FormattedEntityId from the specified Kiji row key.
   *
   * @param kijiRowKey Map of the component name (as specified in key_spec in the layout
   *                   file) to the object containing its value.
   * @param format The RowKeyFormat as specified in the layout file.
   * @return a new FormattedEntityId with the specified Kiji row key.
   */
  public static FormattedEntityId fromKijiRowKey(Map<String, Object> kijiRowKey,
      RowKeyFormat format) {
    // Check that the provided Kiji Row Key fits the encoding format.
    if (!kijiRowKey.keySet().equals(format.getKeySpec().keySet())) {
      throw new RuntimeException("The row key provided does not fit the component key spec. ");
    }
    byte[] hbaserowkey = null;
    try {
      hbaserowkey = makeHbaseRowKey(format, kijiRowKey);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
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
    Map<String, Object> kijiRowKey = null;
    byte[] originalKey = hbaseRowKey.clone();
    try {
      kijiRowKey = makeKijiRowKey(format, hbaseRowKey);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
    return new FormattedEntityId(format, originalKey, kijiRowKey);
  }

  /**
   * Create an hbase row key, which is a byte array from the given formatted kijiRowKey.
   * The following encoding will be used to ensure correct ordering:
   * Strings are UTF-8 encoded and terminated by a null byte.
   * Integers are exactly 4 bytes long.
   * Hashed components are exactly hash_size bytes long.
   * @param format The formatted row key format for this table.
   * @param kijiRowKey A map of component names (or the row key) to their values.
   * @return A byte array representing the encoded Hbase row key.
   * @throws UnsupportedEncodingException If encoding any string component fails.
   */
  private static byte[] makeHbaseRowKey(RowKeyFormat format, Map<String, Object> kijiRowKey)
      throws UnsupportedEncodingException {
    // Ensure that the first component of the resultant key is not null.
    // This means that it is either an IDENTITY transform of an existing
    // not-null field or a HASH transform of a valid non-null field.
    StorageEncoding firstComponent = format.getEncodedKeySpec().get(0);
    if (((KeyTransform.IDENTITY == firstComponent.getTransform())
        && (null == kijiRowKey.get(firstComponent.getComponentName())))
        || ((KeyTransform.HASH == firstComponent.getTransform())
        && (null == kijiRowKey.get(firstComponent.getTarget())))) {
      // This would create an empty hbase row key.
      throw new RuntimeException("Invalid Kiji Row Key");
    }

    ArrayList<byte[]> hbaseKey = new ArrayList<byte[]>();
    // Create the hbase row key based on the encoding specified.
    final byte[] zeroDelim = new byte[] {0};
    for (StorageEncoding enc: format.getEncodedKeySpec()) {
      if ((enc.getTransform() == KeyTransform.IDENTITY)
          && (null == kijiRowKey.get(enc.getComponentName()))) {
        // Elements to the right of the null component must be null, so we stop
        // doing any kind of encoding at this point.
        break;
      }
      switch (enc.getTransform()) {
        // Identity transform simply converts the component into its byte array representation.
        case IDENTITY:
          switch (format.getKeySpec().get(enc.getComponentName())) {
            case STRING:
              if (kijiRowKey.get(enc.getComponentName()).getClass() != String.class) {
                throw new RuntimeException("Mismatch in component type for "
                    + enc.getComponentName());
              } else {
                hbaseKey.add(((String)kijiRowKey.get(enc.getComponentName())).getBytes("UTF-8"));
                hbaseKey.add(zeroDelim);
              }
              break;
            case INTEGER:
              if (kijiRowKey.get(enc.getComponentName()).getClass() != Integer.class) {
                throw new RuntimeException("Mismatch in component type for "
                    + enc.getComponentName());
              } else {
                int temp = (Integer)kijiRowKey.get(enc.getComponentName());
                byte[] tempBytes = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE)
                    .putInt(temp).array();
                tempBytes[0] = (byte)((int)tempBytes[0] ^ (int)Byte.MIN_VALUE);
                hbaseKey.add(tempBytes);
              }
              break;
            case LONG:
              if (kijiRowKey.get(enc.getComponentName()).getClass() != Long.class) {
                throw new RuntimeException("Mismatch in component type for "
                    + enc.getComponentName());
              } else {
                long temp = (Long)kijiRowKey.get(enc.getComponentName());
                byte[] tempBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(temp).array();
                tempBytes[0] = (byte)((int)tempBytes[0] ^ (int)Byte.MIN_VALUE);
                hbaseKey.add(tempBytes);
              }
              break;
            default:
              throw new RuntimeException("Unsupported Component Type");
          }
          break;
        // Create a byte array of length hash_size by hashing the target component.
        case HASH:
          Preconditions.checkNotNull(enc.getHashType());
          Preconditions.checkNotNull(enc.getHashSize());
          Preconditions.checkNotNull(enc.getTarget());
          Object targetComponent = kijiRowKey.get(enc.getTarget());
          String hashTarget;
          if (targetComponent.getClass() == Integer.class) {
            hashTarget = ((Integer)targetComponent).toString();
          } else if (targetComponent.getClass() == Long.class) {
            hashTarget = ((Long)targetComponent).toString();
          } else if (targetComponent.getClass() == String.class) {
            hashTarget = (String)targetComponent;
          } else {
            throw new RuntimeException("Unsupported class for row key component");
          }
          byte[] hashed = Arrays.copyOfRange(Hasher.hash(hashTarget), 0, enc.getHashSize());
          hbaseKey.add(hashed);
          break;

        default:
          throw new RuntimeException("Unsupported Key Transform");
      }
    }

    // create a flat byte array out of the individual components.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (byte[] barr: hbaseKey) {
      baos.write(barr, 0, barr.length);
    }
    return baos.toByteArray();
  }

  @Override
  public byte[] getHBaseRowKey() {
    return mHBaseRowKey;
  }

  @Override
  public Map<String, Object> getKijiRowKey() {
    return mKeyComponentMap;
  }

  /**
   * Convert a byte array containing an hbase row key into a Map corresponding to the key_spec
   * in the layout file.
   * @param format The row key format as specified in the layout file.
   * @param hbaseRowKey A byte array containing the hbase row key.
   * @return a Hashmap with keys corresponding to the component names as specified in key_spec, with
   *         their corresponding values.
   * @throws UnsupportedEncodingException If the string component cannot be converted to UTF-8.
   */
  private static Map<String, Object> makeKijiRowKey(RowKeyFormat format, byte[] hbaseRowKey)
      throws UnsupportedEncodingException {
    if (hbaseRowKey.length == 0) {
      throw new RuntimeException("Invalid hbase row key");
    }
    Map<String, Object> kijiRowKey = new HashMap<String, Object>();
    int pos = 0;
    for (Iterator i = format.getEncodedKeySpec().iterator(); i.hasNext();) {
      StorageEncoding enc = (StorageEncoding)i.next();
      if (pos >= hbaseRowKey.length) {
        do {
          if (enc.getTransform() == KeyTransform.IDENTITY) {
            kijiRowKey.put(enc.getComponentName(), null);
          }
        } while (i.hasNext());
        break;
      }
      ByteBuffer buf;
      switch (enc.getTransform()) {
        // If the next component is not a hash, we need to extract it
        case IDENTITY:
          switch (format.getKeySpec().get(enc.getComponentName())) {
            case STRING:
              // Read the row key until we encounter a Null (0) byte or end.
              int endpos = pos;
              while (endpos < hbaseRowKey.length && (hbaseRowKey[endpos] != (byte)0)) {
                endpos += 1;
              }
              String str = new String(hbaseRowKey, pos, endpos - pos, "UTF-8");
              kijiRowKey.put(enc.getComponentName(), str);
              pos = endpos + 1;
              break;
            case INTEGER:
              // Toggle highest order bit to return to original 2's complement.
              hbaseRowKey[pos] =  (byte)((int)hbaseRowKey[pos] ^ (int)Byte.MIN_VALUE);
              try {
                buf = ByteBuffer.wrap(hbaseRowKey, pos, Integer.SIZE / Byte.SIZE);
              } catch (IndexOutOfBoundsException e) {
                throw new RuntimeException("Malformed hbase Row Key at " + enc.getComponentName());
              }
              kijiRowKey.put(enc.getComponentName(), Integer.valueOf(buf.getInt()));
              pos = pos + Integer.SIZE / Byte.SIZE;
              break;
            case LONG:
              // Toggle highest order bit to return to original 2's complement.
              hbaseRowKey[pos] =  (byte)((int)hbaseRowKey[pos] ^ (int)Byte.MIN_VALUE);
              try {
                buf = ByteBuffer.wrap(hbaseRowKey, pos, Long.SIZE / Byte.SIZE);
              } catch (IndexOutOfBoundsException e) {
                throw new RuntimeException("Malformed hbase Row Key at " + enc.getComponentName());
              }
              kijiRowKey.put(enc.getComponentName(), Long.valueOf(buf.getLong()));
              pos = pos + Long.SIZE / Byte.SIZE;
              break;
            default:
              throw new RuntimeException("Unsupported Component Type");
          }
          break;
        // Discard the hashed component.
        case HASH:
          if (pos + enc.getHashSize() > hbaseRowKey.length) {
            throw new RuntimeException("Malformed hbase Row Key at " + enc.getComponentName());
          }
          pos = pos + enc.getHashSize();
          break;
        default:
          throw new RuntimeException("Unsupported Key Transform");

      }
    }
    return kijiRowKey;
  }

  /**
   * Creates a new FormattedEntityId.
   * @param format Format of the row key as specified in the layout file.
   * @param hbaseRowKey Byte array containing the hbase row key.
   * @param kijiRowKey Map of the kiji row key components to values.
   */
  public FormattedEntityId(RowKeyFormat format, byte[] hbaseRowKey,
      Map<String, Object> kijiRowKey) {
    mFormat = format;
    mHBaseRowKey = hbaseRowKey;
    mKeyComponentMap = kijiRowKey;
  }

  /** {@inheritDoc} */
  @Override
  public RowKeyFormat getFormat() {
    return mFormat;
  }
}
