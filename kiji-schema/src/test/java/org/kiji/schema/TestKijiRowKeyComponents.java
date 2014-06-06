/**
 * (c) Copyright 2013 WibiData, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.junit.Test;

import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.HashSpec;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiRowKeyComponents extends KijiClientTest {
  private RowKeyFormat2 makeFormattedRowKeyFormat() {
    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder().setName("dummy").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("str1").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("str2").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("anint").setType(ComponentType.INTEGER).build(),
        RowKeyComponent.newBuilder().setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().setHashSize(2).build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat makeRawRowKeyFormat() {
    final RowKeyFormat format = RowKeyFormat.newBuilder().setEncoding(RowKeyEncoding.RAW).build();
    return format;
  }

  @Test
  public void testRawKeys() throws Exception {
    EntityIdFactory factory = EntityIdFactory.getFactory(makeRawRowKeyFormat());
    KijiRowKeyComponents krkc = KijiRowKeyComponents.fromComponents("skimbleshanks");

    assertEquals(factory.getEntityId("skimbleshanks"), factory.getEntityId(krkc));

    // Install a table with a raw format and ensure that the checks work out.
    Kiji kiji = getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_UNHASHED));
    KijiTable table = kiji.openTable("table");
    assertEquals(table.getEntityId("skimbleshanks"), factory.getEntityId(krkc));
    assertEquals(table.getEntityId("skimbleshanks"), krkc.getEntityIdForTable(table));
    table.release();
  }

  @Test
  public void testFormattedKeys() throws Exception {
    EntityIdFactory factory = EntityIdFactory.getFactory(makeFormattedRowKeyFormat());
    KijiRowKeyComponents krkc = KijiRowKeyComponents.fromComponents(
        "skimbleshanks",
        "mungojerrie",
        "rumpelteazer",
        5
        /* Last component left as a null */);

    assertEquals(
        factory.getEntityId("skimbleshanks", "mungojerrie", "rumpelteazer", 5, null),
        factory.getEntityId(krkc));

    // Install a table with the same key format and ensure that the checks work out.
    Kiji kiji = getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.FORMATTED_RKF));
    KijiTable table = kiji.openTable("table");
    assertEquals(
        table.getEntityId("skimbleshanks", "mungojerrie", "rumpelteazer", 5),
        factory.getEntityId(krkc));
    assertEquals(
        table.getEntityId("skimbleshanks", "mungojerrie", "rumpelteazer", 5, null),
        krkc.getEntityIdForTable(table));
    table.release();
  }

  @Test
  public void testEquals() throws Exception {
    KijiRowKeyComponents krkc1 = KijiRowKeyComponents.fromComponents(
        "jennyanydots",
        1,
        null,
        null);
    KijiRowKeyComponents krkc2 = KijiRowKeyComponents.fromComponents(
        "jennyanydots",
        1,
        null,
        null);
    KijiRowKeyComponents krkc3 = KijiRowKeyComponents.fromComponents(
        "jennyanydots",
        1,
        null);
    assertFalse(krkc1 == krkc2);
    assertEquals(krkc1, krkc2);
    assertFalse(krkc1.equals(krkc3));

    // byte[] use a different code path.
    byte[] bytes1 = new byte[]{47};
    byte[] bytes2 = new byte[]{47};
    assertFalse(bytes1.equals(bytes2));
    KijiRowKeyComponents krkc4 = KijiRowKeyComponents.fromComponents(bytes1);
    KijiRowKeyComponents krkc5 = KijiRowKeyComponents.fromComponents(bytes2);
    assertEquals(krkc4, krkc5);
  }

  @Test
  public void testHashCode() throws Exception {
    byte[] bytes1 = new byte[]{47};
    byte[] bytes2 = new byte[]{47};
    assertFalse(bytes1.equals(bytes2));
    KijiRowKeyComponents krkc1 = KijiRowKeyComponents.fromComponents(bytes1);
    KijiRowKeyComponents krkc2 = KijiRowKeyComponents.fromComponents(bytes2);
    assertEquals(krkc1.hashCode(), krkc2.hashCode());
  }

  @Test
  public void testFormattedCompare() {
    List<KijiRowKeyComponents> sorted = ImmutableList.of(
        KijiRowKeyComponents.fromComponents("a", null, null),
        KijiRowKeyComponents.fromComponents("a", 123, 456L),
        KijiRowKeyComponents.fromComponents("a", 456, null));

    List<KijiRowKeyComponents> shuffled = Lists.newArrayList(sorted);
    Collections.shuffle(shuffled);
    Collections.sort(shuffled);

    Assert.assertEquals(sorted, shuffled);
  }

  @Test
  public void testRawCompare() {
    final List<KijiRowKeyComponents> sorted = ImmutableList.of(
        KijiRowKeyComponents.fromComponents(new Object[] {new byte[] {0x00, 0x00}}),
        KijiRowKeyComponents.fromComponents(new Object[] {new byte[] {0x00, 0x01}}),
        KijiRowKeyComponents.fromComponents(new Object[] {new byte[] {0x01, 0x00}}));

    List<KijiRowKeyComponents> shuffled = Lists.newArrayList(sorted);
    Collections.shuffle(shuffled);
    Collections.sort(shuffled);

    Assert.assertEquals(sorted, shuffled);
  }
}
