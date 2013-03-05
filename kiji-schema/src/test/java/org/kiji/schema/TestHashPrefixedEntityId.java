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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.kiji.schema.avro.HashType;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.util.ByteArrayFormatter;

/** Tests for HashPrefixedEntityId. */
public class TestHashPrefixedEntityId {
  @Test
  public void testHashedEntityIdFromKijiRowKey() {
    final RowKeyFormat format = RowKeyFormat.newBuilder()
        .setEncoding(RowKeyEncoding.HASH_PREFIX)
        .setHashType(HashType.MD5)
        .setHashSize(4)
        .build();
    final byte[] kijiRowKey = new byte[] {0x11, 0x22};
    final HashPrefixedEntityId eid = HashPrefixedEntityId.getEntityId(kijiRowKey, format);
    assertArrayEquals(kijiRowKey, (byte[])eid.getComponentByIndex(0));
    assertEquals(
        "c700ed4f1122",
        ByteArrayFormatter.toHex(eid.getHBaseRowKey()));
    assertEquals(1, eid.getComponents().size());
    assertEquals(kijiRowKey, eid.getComponents().get(0));
  }

  @Test
  public void testHashedEntityIdFromHBaseRowKey() throws Exception {
    final RowKeyFormat format = RowKeyFormat.newBuilder()
        .setEncoding(RowKeyEncoding.HASH_PREFIX)
        .setHashType(HashType.MD5)
        .setHashSize(4)
        .build();
    final byte[] hbaseRowKey = ByteArrayFormatter.parseHex("c700ed4f1122");
    final HashPrefixedEntityId eid = HashPrefixedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    assertArrayEquals(hbaseRowKey, eid.getHBaseRowKey());
    assertArrayEquals(new byte[] {0x11, 0x22}, (byte[])eid.getComponentByIndex(0));
    assertEquals(1, eid.getComponents().size());
    assertArrayEquals(new byte[] {0x11, 0x22}, (byte[])eid.getComponents().get(0));
  }
}
