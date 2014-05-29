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

package org.kiji.schema;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiRowKeySplitter {

  @Test
  public void testGetRowKeyResolution() throws IOException {
    assertEquals(2, KijiRowKeySplitter
        .getRowKeyResolution(KijiTableLayouts.getLayout(KijiTableLayouts.FORMATTED_RKF)));
    assertEquals(16, KijiRowKeySplitter
        .getRowKeyResolution(KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED)));
  }

  /**
   * It should be possible to create as many regions as we have potential values for a
   * hash prefix (i.e. it's desirable to create 256 regions for a table with a 1-byte
   * hash prefix). In order to do so, the splitting needs to consider the start
   * and end keys as inclusive. This tests validates such a case by asking for 256
   * regions on a 1 byte hash prefix.
   */
  @Test
  public void testSplitKeysInclusive() {
    byte[][] splitKeys = KijiRowKeySplitter.get().getSplitKeys(256, 1);
    assertThat(splitKeys.length, is(255));
  }

  @Test
  public void testSplitKeysSimple() {
    final byte[][] splitKeys = KijiRowKeySplitter.get().getSplitKeys(2, 1);
    assertEquals(1, splitKeys.length);
    assertArrayEquals(new byte[]{(byte) 0x80}, splitKeys[0]);
  }

  @Test
  public void testSplitKeysInvalidArgument() {
    try {
      KijiRowKeySplitter.get().getSplitKeys(257, 1);
      Assert.fail("Should be invalid!");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("Number of regions must be at most 256"));
    }

    try {
      KijiRowKeySplitter.get().getSplitKeys(65537, 2);
      Assert.fail("Should be invalid!");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("Number of regions must be at most 65536"));
    }
  }
}
