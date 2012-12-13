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

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.kiji.annotations.ApiAudience;

/**
 * A thread-safe utility for computing hashes of strings.
 */
@ApiAudience.Private
public final class Hasher {
  /** The number of bytes in a hash. */
  public static final int HASH_SIZE_BYTES = 16;

  /** Name of the 128-bit MD5 algorithm. */
  private static final String ALGORITHM = "MD5";

  /** A thread-local message digest. */
  private static final ThreadLocal<MessageDigest> MESSAGE_DIGEST
      = new ThreadLocal<MessageDigest>() {
          @Override
          protected MessageDigest initialValue() {
            try {
              return MessageDigest.getInstance(ALGORITHM);
            } catch (NoSuchAlgorithmException e) {
              throw new RuntimeException(e);
            }
          }
        };

  /** Disable constructor for utility class. */
  private Hasher() {}

  /**
   * Hashes the input string.
   *
   * @param input The string to hash.
   * @return The 128-bit MD5 hash of the input.
   */
  public static byte[] hash(String input) {
    try {
      return hash(input.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Hashes the input byte array.
   *
   * @param input The bytes to hash.
   * @return The 128-bit MD5 hash of the input.
   */
  public static byte[] hash(byte[] input) {
    return MESSAGE_DIGEST.get().digest(input);
  }
}
