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

/**
 * Useful static classes for converting between ByteBuffers and byte arrays.
 */
public final class CassandraByteUtil {

  /**
   * Turn a ByteBuffer into a byte[].
   *
   * @param byteBuffer to turn into a byte[].
   * @return the resulting byte[].
   */
  public static byte[] byteBuffertoBytes(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);
    return bytes;
  }

  /**
   * Turn a byte[] into a ByteBuffer.
   *
   * @param bytes to turn into a ByteBuffer.
   * @return the ByteBuffer.
   */
  public static ByteBuffer bytesToByteBuffer(byte[] bytes) {
    return ByteBuffer.wrap(bytes);
  }

  /**
   * Private constructor.
   */
  private CassandraByteUtil() {}
}
