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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.kiji.schema.avro.HashType;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.util.ByteArrayFormatter;

/** Tests for HashedEntityId. */
public class TestHashedEntityId {
  @Test
  public void testHashedEntityIdFromKijiRowKey() {
    final RowKeyFormat format = RowKeyFormat.newBuilder()
        .setEncoding(RowKeyEncoding.HASH)
        .setHashType(HashType.MD5)
        .setHashSize(16)
        .build();
    final byte[] kijiRowKey = new byte[] {0x11, 0x22};
    final HashedEntityId eid = HashedEntityId.getEntityId(kijiRowKey, format);
    assertArrayEquals(kijiRowKey, (byte[])eid.getComponentByIndex(0));
    assertEquals(
        "c700ed4fdb1d27055aa3faa2c2432283",
        ByteArrayFormatter.toHex(eid.getHBaseRowKey()));
    assertEquals(1, eid.getComponents().size());
    // when we create a hashed entity ID from a kiji row key we expect to
    // be able to retrieve it.
    assertNotNull(eid.getComponents().get(0));
    assertEquals(kijiRowKey, eid.getComponents().get(0));
  }

  @Test
  public void testHashedEntityIdFromHBaseRowKey() throws Exception {
    final RowKeyFormat format = RowKeyFormat.newBuilder()
        .setEncoding(RowKeyEncoding.HASH)
        .setHashType(HashType.MD5)
        .setHashSize(16)
        .build();
    final byte[] hbaseRowKey = ByteArrayFormatter.parseHex("c700ed4fdb1d27055aa3faa2c2432283");
    final HashedEntityId eid = HashedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    assertArrayEquals(hbaseRowKey, eid.getHBaseRowKey());
    // when we create a hashed entity ID from an hbase row key we cannot retrieve the
    // original key which was used to create it.
    try {
      eid.getComponentByIndex(0);
      fail("Should fail with IllegalStateException");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot retrieve components as materialization is suppressed", ise.getMessage());
    }
    try {
      eid.getComponents();
      fail("Should fail with IllegalStateException");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot retrieve components as materialization is suppressed", ise.getMessage());
    }
  }
}
