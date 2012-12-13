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

import java.io.IOException;

import org.kiji.annotations.ApiAudience;

/** Utility class to format byte arrays. */
@ApiAudience.Private
public final class ByteArrayFormatter {
  /** Utility class cannot be instantiated. */
  private ByteArrayFormatter() {
  }

  /**
   * Formats a byte array as a sequence of hex-digits with no separator.
   *
   * @param bytes Byte array to format.
   * @return the hex string representation of the byte array.
   */
  public static String toHex(byte[] bytes) {
    final StringBuilder sb = new StringBuilder();
    for (byte data : bytes) {
      sb.append(String.format("%02x", data));
    }
    return sb.toString();
  }

  /**
   * Formats a byte array as a sequence of 2 hex-digits bytes with the specified separator.
   *
   * @param bytes Byte array to format.
   * @param separator Separator used between each byte.
   * @return string the hex representation of the byte array.
   */
  public static String toHex(byte[] bytes, char separator) {
    final String byteFormat = "%02x" + separator;
    final StringBuilder sb = new StringBuilder();
    for (byte b: bytes) {
      sb.append(String.format(byteFormat, b));
    }
    // Remove the final separator, if any:
    final int newLength = sb.length() - 1;
    if (newLength >= 0) {
      sb.setLength(newLength);
    }
    return sb.toString();
  }

  /**
   * Formats a byte array as a URL.
   *
   * @param bytes Byte array to format;
   * @return the URL encoding of the byte array.
   */
  public static String toURL(byte[] bytes) {
    final StringBuilder sb = new StringBuilder();
    for (byte data : bytes) {
      sb.append('%').append(String.format("%02x", data));
    }
    return sb.toString();
  }

  /**
   * Parses a string hexadecimal representation of a byte array with separator.
   *
   * @param hex String hexadecimal representation of the byte array to parse.
   * @param separator Separator used in the string.
   * @return the parsed byte array.
   * @throws IOException on parse error.
   */
  public static byte[] parseHex(String hex, char separator) throws IOException {
    final int nbytes = (hex.length() + 1) / 3;
    if (hex.length() != (nbytes * 3) - 1) {
      throw new IOException(String.format("Invalid byte sequence '%s'.", hex));
    }
    final byte[] bytes = new byte[nbytes];

    int pos = 0;
    int index = 0;
    while (pos < hex.length()) {
      final int hiDigit = Character.digit(hex.charAt(pos), 16);
      final int loDigit = Character.digit(hex.charAt(pos + 1), 16);
      if ((hiDigit == -1) || (loDigit == -1)) {
        throw new IOException(String.format(
            "Invalid byte sequence '%s': error at position %d.", hex, pos));
      }
      bytes[index] = (byte) ((hiDigit << 4) + loDigit);
      index += 1;

      pos += 2;
      if (pos < hex.length()) {
        if (hex.charAt(pos) != separator) {
          throw new IOException(String.format(
              "Invalid byte sequence '%s': expecting separator '%s' at position %d.",
              hex, separator, pos));
        }
        pos += 1;
      }
    }
    return bytes;
  }

  /**
   * Parses a string hexadecimal representation of a byte array.
   *
   * @param hex String hexadecimal representation of the byte array to parse.
   * @return the parsed byte array.
   * @throws IOException on parse error.
   */
  public static byte[] parseHex(String hex) throws IOException {
    if (hex.length() % 2 != 0) {
      throw new IOException(String.format("Invalid byte sequence '%s'.", hex));
    }
    final int nbytes = hex.length() / 2;
    final byte[] bytes = new byte[nbytes];

    int pos = 0;
    int index = 0;
    while (pos < hex.length()) {
      final int hiDigit = Character.digit(hex.charAt(pos), 16);
      final int loDigit = Character.digit(hex.charAt(pos + 1), 16);
      if ((hiDigit == -1) || (loDigit == -1)) {
        throw new IOException(String.format(
            "Invalid byte sequence '%s': error at position %d.", hex, pos));
      }
      bytes[index] = (byte) ((hiDigit << 4) + loDigit);
      index += 1;

      pos += 2;
    }
    return bytes;
  }

}
