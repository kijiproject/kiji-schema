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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestSplitKeyFile {

  @Test
  public void testDecodeSplitKeyFile() throws Exception {
    final String content =
        "key1\n"
        + "key2\n";
    final List<byte[]> keys =
        SplitKeyFile.decodeRegionSplitList(new ByteArrayInputStream(Bytes.toBytes(content)));
    assertEquals(2, keys.size());
    assertArrayEquals(Bytes.toBytes("key1"), keys.get(0));
    assertArrayEquals(Bytes.toBytes("key2"), keys.get(1));
  }

  @Test
  public void testDecodeSplitKeyFileNoEndNewLine() throws Exception {
    final String content =
        "key1\n"
        + "key2";
    final List<byte[]> keys =
        SplitKeyFile.decodeRegionSplitList(new ByteArrayInputStream(Bytes.toBytes(content)));
    assertEquals(2, keys.size());
    assertArrayEquals(Bytes.toBytes("key1"), keys.get(0));
    assertArrayEquals(Bytes.toBytes("key2"), keys.get(1));
  }

  @Test
  public void testDecodeRowKey() throws Exception {
    assertArrayEquals(Bytes.toBytes("this is a \n key"),
        SplitKeyFile.decodeRowKey("this is a \n key"));

    assertArrayEquals(Bytes.toBytes("this is a \\ key"),
        SplitKeyFile.decodeRowKey("this is a \\\\ key"));

    assertArrayEquals(Bytes.toBytes("this is a \n key"),
        SplitKeyFile.decodeRowKey("this is a \\x0A key"));

    assertArrayEquals(Bytes.toBytes("this is a \n key"),
        SplitKeyFile.decodeRowKey("this is a \\x0a key"));
  }

  @Test(expected = IOException.class)
  public void testDecodeRowKeyInvalidHexEscape() throws Exception {
    SplitKeyFile.decodeRowKey("this is a \\xZZ key");
  }

  @Test(expected = IOException.class)
  public void testDecodeRowKeyInvalidEscape() throws Exception {
    // \n is escaped as \x0a
    SplitKeyFile.decodeRowKey("this is a \\n key");
  }

  @Test(expected = IOException.class)
  public void testDecodeRowKeyUnterminatedEscape() throws Exception {
    SplitKeyFile.decodeRowKey("this is a \\");
  }

  @Test(expected = IOException.class)
  public void testDecodeRowKeyInvalidHex() throws Exception {
    SplitKeyFile.decodeRowKey("this is a \\x-6");
  }

  @Test(expected = IOException.class)
  public void testDecodeRowKeyIncompleteHex() throws Exception {
    SplitKeyFile.decodeRowKey("this is a \\x6");
  }
}
