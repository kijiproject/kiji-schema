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
import java.util.Arrays;

import org.kiji.annotations.ApiAudience;

/**
 * Wraps a byte array of binary-encoded data into a byte stream.
 *
 * Provides various binary decoding functions.
 * Heavily inspired from protocol buffer's CodedOutputStream and Avro's BinaryDecoder.
 */
@ApiAudience.Private
public final class ByteStreamArray {
  /** Raised when decoding some data fails (eg. a variable-length integer). */
  @ApiAudience.Private
  public static final class EncodingException extends IOException {
  }

  private byte[] mBytes;
  private int mOffset;

  /**
   * Constructs a new stream of byte.
   *
   * @param bytes Array of byte to wrap into a stream.
   * @param offset Offset of the stream head.
   */
  public ByteStreamArray(byte[] bytes, int offset) {
    this.mBytes = Arrays.copyOf(bytes, bytes.length);
    this.mOffset = offset;
  }

  /**
   * Constructs a new stream of byte.
   *
   * @param bytes Array of byte to wrap into a stream.
   */
  public ByteStreamArray(byte[] bytes) {
    this(bytes, 0);
  }

  /**
   * Reports the size of the zig-zag encoding of the specified long integer.
   *
   * @param number Long integer to encode.
   * @return Number of bytes required to encode the long using the zig-zag var-int64 encoding.
   *     Between 1 and 10 bytes.
   */
  public static int sizeOfLongAsVarInt64(long number) {
    if ((number & ~0x000000000000007fL) == 0) {
      return 1;
    }
    if ((number & ~0x0000000000003fffL) == 0) {
      return 2;
    }
    if ((number & ~0x00000000001fffffL) == 0) {
      return 3;
    }
    if ((number & ~0x000000000fffffffL) == 0) {
      return 4;
    }
    if ((number & ~0x00000007ffffffffL) == 0) {
      return 5;
    }
    if ((number & ~0x000003ffffffffffL) == 0) {
      return 6;
    }
    if ((number & ~0x0001ffffffffffffL) == 0) {
      return 7;
    }
    if ((number & ~0x00ffffffffffffffL) == 0) {
      return 8;
    }
    if ((number & ~0x7fffffffffffffffL) == 0) {
      return 9;
    }
    return 10;
  }

  /**
   * Serializes a long integer into bytes using the zig-zag variable-length encoding scheme.
   *
   * @param number Long integer to encode.
   * @return Zig-zag encoded long, as an array of up to 10 bytes.
   */
  public static byte[] longToVarInt64(long number) {
    final int nbytes = sizeOfLongAsVarInt64(number);
    final byte[] bytes = new byte[nbytes];

    int pos = 0;
    if ((number & ~0x7f) != 0) {  // first test is specific (eg. if number == MaxLong)
      bytes[pos++] = (byte)(0x80 | (number & 0x7f));
      number >>>= 7;
      if (number > 0x7f) {
        bytes[pos++] = (byte)(0x80 | (number & 0x7f));
        number >>>= 7;
        if (number > 0x7f) {
          bytes[pos++] = (byte)(0x80 | (number & 0x7f));
          number >>>= 7;
          if (number > 0x7f) {
            bytes[pos++] = (byte)(0x80 | (number & 0x7f));
            number >>>= 7;
            if (number > 0x7f) {
              bytes[pos++] = (byte)(0x80 | (number & 0x7f));
              number >>>= 7;
              if (number > 0x7f) {
                bytes[pos++] = (byte)(0x80 | (number & 0x7f));
                number >>>= 7;
                if (number > 0x7f) {
                  bytes[pos++] = (byte)(0x80 | (number & 0x7f));
                  number >>>= 7;
                  if (number > 0x7f) {
                    bytes[pos++] = (byte)(0x80 | (number & 0x7f));
                    number >>>= 7;
                    if (number > 0x7f) {
                      bytes[pos++] = (byte)(0x80 | (number & 0x7f));
                      number >>>= 7;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    bytes[pos] = (byte) number;
    return bytes;
  }

  /**
   * Serializes a long integer into bytes using the zig-zag variable-length encoding scheme.
   *
   * @param number Long integer to encode.
   * @return Zig-zag encoded long, as an array of up to 10 bytes.
   */
  public static byte[] longToZigZagVarInt64(long number) {
    // Zig-zag encode: move sign to low-order bit, and flip others if negative
    number = (number << 1) ^ (number >> 63);
    return longToVarInt64(number);
  }

  /**
   * Reads a variable-length zig-zag encoded signed integer up to 64 bits.
   *
   * @return the read integer as a long.
   * @throws EncodingException on decoding error.
   */
  public long readVarInt64() throws EncodingException {
    int bits = mBytes[mOffset++] & 0xff;
    long acc = bits & 0x7f; // bits accumulator
    if (bits > 0x7f) {
      bits = mBytes[mOffset++] & 0xff;
      acc ^= (bits & 0x7f) << 7;
      if (bits > 0x7f) {
        bits = mBytes[mOffset++] & 0xff;
        acc ^= (bits & 0x7f) << 14;
        if (bits > 0x7f) {
          bits = mBytes[mOffset++] & 0xff;
          acc ^= (bits & 0x7f) << 21;
          if (bits > 0x7f) {
            bits = mBytes[mOffset++] & 0xff;
            acc ^= (bits & 0x7fL) << 28;
            if (bits > 0x7f) {
              bits = mBytes[mOffset++] & 0xff;
              acc ^= (bits & 0x7fL) << 35;
              if (bits > 0x7f) {
                bits = mBytes[mOffset++] & 0xff;
                acc ^= (bits & 0x7fL) << 42;
                if (bits > 0x7f) {
                  bits = mBytes[mOffset++] & 0xff;
                  acc ^= (bits & 0x7fL) << 49;
                  if (bits > 0x7f) {
                    bits = mBytes[mOffset++] & 0xff;
                    acc ^= (bits & 0x7fL) << 56;
                    if (bits > 0x7f) {
                      bits = mBytes[mOffset++] & 0xff;
                      acc ^= (bits & 0x7fL) << 63;
                      if (bits > 0x7f) {
                        throw new EncodingException();
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    return acc;
  }

  /**
   * Reads a variable-length zig-zag encoded signed integer up to 64 bits.
   *
   * @return the read integer as a long.
   * @throws EncodingException on decoding error.
   */
  public long readZigZagVarInt64() throws EncodingException {
    final long vint64 = readVarInt64();
    return (vint64 >>> 1) ^ -(vint64 & 1); // decode zig-zag back to two's-complement
  }

  /**
   * Skips some bytes.
   *
   * @param nbytes Number of bytes to skip.
   */
  public void skip(int nbytes) {
    this.mOffset += nbytes;
  }

  /**
   * Reads the specified number of bytes in the stream.
   *
   * @param nbytes Number of bytes to read.
   * @return Bytes read.
   */
  public byte[] readBytes(int nbytes) {
    final int fromOffset = this.mOffset;
    this.mOffset += nbytes;
    return Arrays.copyOfRange(this.mBytes, fromOffset, this.mOffset);
  }

  /** @return a copy of the wrapped byte array. */
  public byte[] getBytes() {
    return Arrays.copyOf(this.mBytes, this.mBytes.length);
  }

  /** @return the current offset in the wrapped byte array. */
  public int getOffset() {
    return this.mOffset;
  }
}
