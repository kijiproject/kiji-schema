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
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;

/**
 * Parses region boundaries split files.
 *
 * <li> The file contains one row key per line.
 * <li> Row keys are encoded in ASCII.
 * <li> Non printable characters must be escaped in hexadecimal "\x??".
 * <li> Backslash must be escaped (doubled) "\\"
 *
 * <p>
 * If a file contains N split keys, N+1 regions will be created, since the first region will contain
 * everything before the first split key, and the last region will contain everything after the last
 * split key.
 */
@ApiAudience.Private
public final class SplitKeyFile {
  /** Utility class. */
  private SplitKeyFile() {
  }

  /**
   * Constructs a split key file from an input stream.  This object will take ownership of
   * the inputStream, which you should clean up by calling close().
   *
   * @param inputStream The file contents.
   * @return the region boundaries, as a list of row keys.
   * @throws IOException on I/O error.
   */
  public static List<byte[]> decodeRegionSplitList(InputStream inputStream) throws IOException {
    try {
      final String content =
          Bytes.toString(IOUtils.toByteArray(Preconditions.checkNotNull(inputStream)));
      final String[] encodedKeys = content.split("\n");
      final List<byte[]> keys = Lists.newArrayListWithCapacity(encodedKeys.length);
      for (String encodedKey : encodedKeys) {
        keys.add(decodeRowKey(encodedKey));
      }
      return keys;
    } finally {
      ResourceUtils.closeOrLog(inputStream);
    }
  }

  /**
   * Decodes a string encoded row key.
   *
   * @param encoded Encoded row key.
   * @return the row key, as a byte array.
   * @throws IOException on I/O error.
   */
  public static byte[] decodeRowKey(String encoded) throws IOException {
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    int index = 0;
    final byte[] bytes = Bytes.toBytes(encoded);
    while (index < bytes.length) {
      final byte data = bytes[index++];

      if (data != '\\') {
        os.write(data);
      } else {
        if (index == bytes.length) {
          throw new IOException(String.format(
              "Invalid trailing escape in encoded row key: '%s'.", encoded));
        }
        final byte escaped = bytes[index++];

        switch (escaped) {
        case '\\': {
          // Escaped backslash:
          os.write('\\');
          break;
        }
        case 'x': {
          // Escaped byte in hexadecimal:
          if (index + 1 >= bytes.length) {
            throw new IOException(String.format(
                "Invalid hexadecimal escape in encoded row key: '%s'.", encoded));
          }
          final String hex = Bytes.toString(Arrays.copyOfRange(bytes, index, index + 2));
          try {
            final int decodedByte = Integer.parseInt(hex, 16);
            if ((decodedByte < 0) || (decodedByte > 255)) {
              throw new IOException(String.format(
                  "Invalid hexadecimal escape in encoded row key: '%s'.", encoded));
            }
            os.write(decodedByte);
          } catch (NumberFormatException nfe) {
            throw new IOException(String.format(
                "Invalid hexadecimal escape in encoded row key: '%s'.", encoded));
          }
          index += 2;
          break;
        }
        default:
          throw new IOException(String.format("Invalid escape in encoded row key: '%s'.", encoded));
        }
      }
   }
    return os.toByteArray();
  }
}
