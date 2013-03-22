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

import java.util.Arrays;

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Wraps an array of bytes into an object that can be used as key in a hash map.
 * The wrapper assumes the byte array is immutable (ie. does not copy the byte array).
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class BytesKey {
  private final byte[] mBytes;

  /**
   * Wraps the given byte array.
   *
   * @param bytes Byte array to wrap.
   */
  public BytesKey(byte[] bytes) {
    this.mBytes = Preconditions.checkNotNull(bytes);
  }

  /** @return The byte array. */
  public byte[] getBytes() {
    return Arrays.copyOf(this.mBytes, this.mBytes.length);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BytesKey)) {
      return false;
    }
    return Arrays.equals(mBytes, ((BytesKey) other).mBytes);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(mBytes);
  }

  @Override
  public String toString() {
    return ByteArrayFormatter.toHex(mBytes, ':');
  }
}
