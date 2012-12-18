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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.Writable;

public final class WritableTestUtil {
  private WritableTestUtil() { }

  public static <T extends Writable> void assertWritable(T expected, Class<T> type)
      throws Exception {
    final T base = type.newInstance();
    testWritable(expected, base);
    testWritable(expected, base);
  }

  private static <T extends Writable> void testWritable(T expected, T actual)
      throws Exception {
    final ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    expected.write(new DataOutputStream(bytesOut));

    final byte[] bytes = bytesOut.toByteArray();

    final ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes);
    actual.readFields(new DataInputStream(bytesIn));

    assertEquals(expected, actual);
  }
}
