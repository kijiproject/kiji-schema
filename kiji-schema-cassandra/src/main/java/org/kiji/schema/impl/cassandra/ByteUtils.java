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

package org.kiji.schema.impl.cassandra;

import java.nio.ByteBuffer;

import com.google.common.base.Charsets;

/**
 * TODO: Remove this file and use the version in kiji-commons.
 */
public final class ByteUtils {

  /**
   * Convert a {@link String} to a UTF-8 encoded {@code byte[]}. This method will copy the bytes
   * from the {@code String}, so modifying the resulting bytes will not modify the original
   * {@code String}.
   *
   * @param string A string to convert to a UTF-8 encoded {@code byte[]}.
   * @return A UTF-8 encoded {@code byte[]}.
   */
  public static byte[] toBytes(final String string) {
    return string.getBytes(Charsets.UTF_8);
  }

  /**
   * Copies the remaining bytes from a {@link java.nio.ByteBuffer} to a {@code byte[]}. After this
   * method returns, the buffer's position will equal its limit.
   *
   * @param buffer A {@code ByteBuffer} to copy remaining bytes from.
   * @return A {@code byte[]} holding the copied bytes.
   */
  public static byte[] toBytes(final ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  /**
   * Convert a UTF-8 encoded {@code byte[]} to a {@link String}. Do *not* call this method on
   * non-UTF-8 encoded {@code byte[]}s. This method will copy the {@code byte[]}, so modifying the
   * {@code byte[]} will not modify the returned {@code String}.
   *
   * @param bytes UTF-8 encoded {@code byte[]} to convert to a String.
   * @return A {@code String}.
   */
  public static String toString(final byte[] bytes) {
    return new String(bytes, Charsets.UTF_8);
  }

  /**
   * Write a printable representation of a byte array.
   *
   * Copied from HBase's {@code org.apache.hadoop.hbase.util.Bytes} class.
   *
   * @param b byte array
   * @return string
   */
  public static String toStringBinary(final byte [] b) {
    if (b == null) {
      return "null";
    }
    return toStringBinary(b, 0, b.length);
  }

  /**
   * Write a printable representation of a byte array. Non-printable
   * characters are hex escaped in the format \\x%02X, eg:
   * \x00 \x05 etc
   *
   * Copied from HBase's {@code org.apache.hadoop.hbase.util.Bytes} class.
   *
   * @param b array to write out
   * @param off offset to start at
   * @param len length to write
   * @return string output
   */
  public static String toStringBinary(final byte [] b, int off, int len) {
    StringBuilder result = new StringBuilder();
    // Just in case we are passed a 'len' that is > buffer length...
    if (off >= b.length) {
      return result.toString();
    }
    if (off + len > b.length) {
      len = b.length - off;
    }
    for (int i = off; i < off + len; ++i) {
      int ch = b[i] & 0xFF;
      if ((ch >= '0' && ch <= '9')
          || (ch >= 'A' && ch <= 'Z')
          || (ch >= 'a' && ch <= 'z')
          || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0) {
        result.append((char)ch);
      } else {
        result.append(String.format("\\x%02X", ch));
      }
    }
    return result.toString();
  }

  /** Private constructor for utility class. */
  private ByteUtils() { }
}
