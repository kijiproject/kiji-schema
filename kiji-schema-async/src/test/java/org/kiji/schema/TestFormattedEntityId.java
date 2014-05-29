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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.SortedMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.HashSpec;
import org.kiji.schema.avro.HashType;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.ByteArrayFormatter;

/** Tests for FormattedEntityId. */
public class TestFormattedEntityId extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFormattedEntityId.class);

  private RowKeyFormat2 makeRowKeyFormat() {
    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder().setName("astring").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("anint").setType(ComponentType.INTEGER).build(),
        RowKeyComponent.newBuilder().setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeStringRowKeyFormat() {
    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder().setName("astring").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("bstring").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("cstring").setType(ComponentType.STRING).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeRowKeyFormatNoNulls() {
    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder().setName("astring").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("anint").setType(ComponentType.INTEGER).build(),
        RowKeyComponent.newBuilder().setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setNullableStartIndex(components.size())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeIntRowKeyFormat() {
    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().setHashSize(8).build())
        .setNullableStartIndex(components.size())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeOrderingTestRowKeyFormat() {
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

  private RowKeyFormat2 makeCompositeHashRowKeyFormat() {
    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder().setName("astring").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("anint").setType(ComponentType.INTEGER).build(),
        RowKeyComponent.newBuilder().setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setRangeScanStartIndex(2)
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeHashedRowKeyFormat() {
    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder().setName("astring").setType(ComponentType.STRING).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder()
            .setSuppressKeyMaterialization(true)
            .setHashSize(components.size())
            .build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeCompletelyHashedRowKeyFormat() {
    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder().setName("astring").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("anint").setType(ComponentType.INTEGER).build(),
        RowKeyComponent.newBuilder().setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setRangeScanStartIndex(components.size())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeSuppressMaterializationTestRKF() {
    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder().setName("astring").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("anint").setType(ComponentType.INTEGER).build(),
        RowKeyComponent.newBuilder().setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().setSuppressKeyMaterialization(true).build())
        .setRangeScanStartIndex(2)
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeDefaultSaltRowKeyFormat() {
    // This does not set the 'salt' element of RowKeyFormat2, leaving it to its default value.

    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder().setName("astring").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("anint").setType(ComponentType.INTEGER).build(),
        RowKeyComponent.newBuilder().setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setComponents(components)
        .build();

    return format;
  }

  public static FormattedEntityId makeId(RowKeyFormat2 format, Object... components) {
    final FormattedEntityId eid =
        FormattedEntityId.getEntityId(Lists.newArrayList(components), format);

    final byte[] hbaseRowKey = eid.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    LOG.info("HBase row key for {} is '{}'.",
        Lists.newArrayList(components),
        ByteArrayFormatter.toHex(hbaseRowKey, ':'));

    return eid;
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testFormattedEntityId() {
    final RowKeyFormat2 format = makeRowKeyFormat();
    final FormattedEntityId formattedEntityId = makeId(format, "one", 1, 7L);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    final List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals("one", actuals.get(0));
    assertEquals(1, actuals.get(1));
    assertEquals(7L, actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testFormattedEntityIdUsingFactory() {
    final RowKeyFormat2 format = makeRowKeyFormat();
    final EntityIdFactory entityIdFactory = EntityIdFactory.getFactory(format);
    final List<Object> inputRowKey = Lists.<Object>newArrayList("one", 1, 7L);

    // passing args as list
    final EntityId formattedEntityId = entityIdFactory.getEntityId(inputRowKey);
    final byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    LOG.info("Hbase Key is: {}", ByteArrayFormatter.toHex(hbaseRowKey, ':'));

    EntityId testEntityId = entityIdFactory.getEntityIdFromHBaseRowKey(hbaseRowKey);
    final List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals("one", actuals.get(0));
    assertEquals(1, actuals.get(1));
    assertEquals(7L, actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());

    // passing args as varargs
    EntityId formattedEntityId2 = entityIdFactory.getEntityId("two",
        new Integer(2), new Long(72L));
    byte[] hbaseRowKey2 = formattedEntityId2.getHBaseRowKey();
    assertNotNull(hbaseRowKey2);

    EntityId testEntityId2 = entityIdFactory.getEntityIdFromHBaseRowKey(hbaseRowKey2);
    List<Object> actuals2 = testEntityId2.getComponents();
    assertEquals(format.getComponents().size(), actuals2.size());
    assertEquals("two", actuals2.get(0));
    assertEquals(2, actuals2.get(1));
    assertEquals(72L, actuals2.get(2));

    assertArrayEquals(formattedEntityId2.getHBaseRowKey(), testEntityId2.getHBaseRowKey());
  }

  @Test
  public void testOtherHashSizes() {
    final RowKeyFormat2 format = makeIntRowKeyFormat();
    final FormattedEntityId formattedEntityId = makeId(format, 1);
    final byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    final List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(1, actuals.get(0));
    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());

  }

  @Test
  public void testFormattedEntityIdWithNull() {
    final RowKeyFormat2 format = makeRowKeyFormat();
    final FormattedEntityId formattedEntityId = makeId(format, "one", 1, null);
    final byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    final List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals("one", actuals.get(0));
    assertEquals(1, actuals.get(1));
    assertNull(actuals.get(2));

    // another way of doing nulls
    final FormattedEntityId formattedEntityId1 = makeId(format, "one", 1);
    assertArrayEquals(formattedEntityId.getHBaseRowKey(), formattedEntityId1.getHBaseRowKey());
  }

  @Test
  public void testFormattedEntityIdOrdering() {
    final RowKeyFormat2 rkf = makeOrderingTestRowKeyFormat();
    final List<FormattedEntityId> expected = Lists.newArrayList();

    // We expect this to be the ordering for the keys below.
    // x is the dummy key to make all hashes equal.
    expected.add(makeId(rkf, "x", "a", "bc"));
    expected.add(makeId(rkf, "x", "a0", "aa"));
    expected.add(makeId(rkf, "x", "a0", "aa", -1));
    expected.add(makeId(rkf, "x", "a0", "aa", 0));
    expected.add(makeId(rkf, "x", "a0", "aa", Integer.MAX_VALUE));
    expected.add(makeId(rkf, "x", "a0", "aa0", -1));
    expected.add(makeId(rkf, "x", "a0", "aa0", 0));
    expected.add(makeId(rkf, "x", "a0", "aa0", 0, Long.MIN_VALUE));
    expected.add(makeId(rkf, "x", "a0", "aa0", 0, 0L));
    expected.add(makeId(rkf, "x", "a0", "aa0", 0, Long.MAX_VALUE));
    expected.add(makeId(rkf, "x", "a0", "aa0", Integer.MAX_VALUE));

    final SortedMap<byte[], FormattedEntityId> hashedOrder =
        Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

    for (FormattedEntityId fid : expected) {
      hashedOrder.put(fid.getHBaseRowKey(), fid);
    }

    LOG.info("Validating HBase row keys ordering:");
    int index = 0;
    for (byte[] key : hashedOrder.keySet()) {
      LOG.info("HBase row key #{} is '{}'", index, ByteArrayFormatter.toHex(key, ':'));
      assertEquals(expected.get(index), hashedOrder.get(key));
      index += 1;
    }
  }

  @Test
  public void testCompositeHashEntityId() {
    final RowKeyFormat2 format = makeCompositeHashRowKeyFormat();
    final FormattedEntityId formattedEntityId = makeId(format, "one", 1, 7L);
    final byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();

    final FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    final List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals("one", actuals.get(0));
    assertEquals(1, actuals.get(1));
    assertEquals(7L, actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testEmptyStringFormattedEntityId() {
    final RowKeyFormat2 format = makeStringRowKeyFormat();
    final FormattedEntityId formattedEntityId = makeId(format, "", "");
    final byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    final List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals("", actuals.get(0));
    assertEquals("", actuals.get(1));
    assertEquals(null, actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());

  }

  @Test
  public void testNullValueInHash() {
    final RowKeyFormat2 format = makeCompletelyHashedRowKeyFormat();

    // Set one component as null explicitly, do not include the next one at all:
    final FormattedEntityId formattedEntityId = makeId(format, "one", null);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    final List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals("one", actuals.get(0));
    assertNull(actuals.get(1));
    assertNull(actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testExplicitNullValueInHash() {
    final RowKeyFormat2 format = makeCompletelyHashedRowKeyFormat();

    FormattedEntityId formattedEntityId = makeId(format, "one", null, null);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    final List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals("one", actuals.get(0));
    assertNull(actuals.get(1));
    assertNull(actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testImplicitNullValueInHash() {
    final RowKeyFormat2 format = makeCompletelyHashedRowKeyFormat();

    // Implicit nulls for remaining components:
    FormattedEntityId formattedEntityId = makeId(format, "one");
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    final List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals("one", actuals.get(0));
    assertNull(actuals.get(1));
    assertNull(actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testSuppressMaterialization() {
    final RowKeyFormat2 format = makeSuppressMaterializationTestRKF();

    FormattedEntityId formattedEntityId = makeId(format, "one", 1, 7L);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testSuppressKeyMaterializationInKiji() throws IOException {
    final Kiji kiji = getKiji(); // owned by KijiClientTest
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.HASHED_FORMATTED_RKF));

    final KijiTable table = kiji.openTable("table");
    try {
      // Write one cell:
      final KijiTableWriter writer = table.openTableWriter();
      try {
        writer.put(table.getEntityId("x"), "family", "column", "1");
      } finally {
        writer.close();
      }

      // Read the written cell back:
      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().withMaxVersions(1).add("family", "column"))
            .build();

        final KijiRowData result = reader.get(table.getEntityId("x"), dataRequest);
        assertTrue(result.containsColumn("family", "column"));

        final String res = result.getMostRecentValue("family", "column").toString();
        assertEquals("1", res);
        assertArrayEquals(
            table.getEntityId("x").getHBaseRowKey(),
            result.getEntityId().getHBaseRowKey());

        try {
          result.getEntityId().getComponents();
          Assert.fail("Should not be able to retrieve components from a hashed entity ID!");
        } catch (IllegalStateException ise) {
          LOG.info("Expected exception: {}", ise.getMessage());
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testSuppressMaterializationComponentsException() {
    final RowKeyFormat2 format = makeSuppressMaterializationTestRKF();
    FormattedEntityId formattedEntityId = makeId(format, "one", 1, 7L);
    try {
      formattedEntityId.getComponents();
      fail("Should fail with IllegalStateException");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot retrieve components as materialization is suppressed", ise.getMessage());
    }
  }

  @Test
  public void testSuppressMaterializationComponentsByIndexException() {
    final RowKeyFormat2 format = makeSuppressMaterializationTestRKF();
    FormattedEntityId formattedEntityId = makeId(format, "one", 1, 7L);
    try {
      formattedEntityId.getComponentByIndex(0);
      fail("Should fail with IllegalStateException");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot retrieve components as materialization is suppressed", ise.getMessage());
    }
  }

  @Test
  public void testSuppressMaterializationComponentsExceptionFromHbaseKey() {
    final RowKeyFormat2 format = makeSuppressMaterializationTestRKF();
    FormattedEntityId formattedEntityId = makeId(format, "one", 1, 7L);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    try {
      testEntityId.getComponents();
      fail("Should fail with IllegalStateException");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot retrieve components as materialization is suppressed", ise.getMessage());
    }
  }

  @Test
  public void testSuppressMaterializationComponentsByIndexExceptionFromHbaseKey() {
    final RowKeyFormat2 format = makeSuppressMaterializationTestRKF();
    FormattedEntityId formattedEntityId = makeId(format, "one", 1, 7L);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);

    try {
      testEntityId.getComponentByIndex(0);
      fail("Should fail with IllegalStateException");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot retrieve components as materialization is suppressed", ise.getMessage());
    }
  }

  @Test
  public void testBadNullFormattedEntityId() {
    final RowKeyFormat2 format = makeRowKeyFormat();

    // No non-null component is allowed after a null component:
    try {
      makeId(format, "one", null, 7L);
      fail("Should fail with EntityIdException");
    } catch (EntityIdException eie) {
      assertEquals("Non null component follows null component", eie.getMessage());
    }
  }

  @Test
  public void testTooManyComponentsFormattedEntityId() {
    final RowKeyFormat2 format = makeRowKeyFormat();
    try {
      makeId(format, "one", 1, 7L, null);
      fail("Should fail with EntityIdException");
    } catch (EntityIdException eie) {
      assertEquals("Too many components in kiji Row Key", eie.getMessage());
    }

  }

  @Test
  public void testWrongComponentTypeFormattedEntityId() {
    final RowKeyFormat2 format = makeRowKeyFormat();
    try {
      // Second component must be an integer:
      makeId(format, "one", 1L, 7L);
      fail("Should fail with EntityIdException");
    } catch (EntityIdException eie) {
      assertEquals("Invalid type for component 1 at index 1 in kijiRowKey", eie.getMessage());
    }
  }

  @Test
  public void testUnexpectedNullFormattedEntityId() {
    final RowKeyFormat2 format = makeRowKeyFormatNoNulls();
    try {
      makeId(format, "one", null);
      fail("Should fail with EntityIdException");
    } catch (EntityIdException eie) {
      assertEquals("Too few components in kiji Row key", eie.getMessage());
    }
  }

  @Test
  public void testTooFewComponentsFormattedEntityId() {
    final RowKeyFormat2 format = makeRowKeyFormatNoNulls();
    try {
      makeId(format, "one");
      fail("Should fail with EntityIdException");
    } catch (EntityIdException eie) {
      assertEquals("Too few components in kiji Row key", eie.getMessage());
    }
  }

  @Test
  public void testBadHbaseKey() {
    final RowKeyFormat2 format = makeIntRowKeyFormat();
    final byte[] hbkey = new byte[] { 0x01, 0x02, 0x042, 0x01, 0x02, 0x042, 0x01, 0x02, 0x042, };

    try {
      FormattedEntityId.fromHBaseRowKey(hbkey, format);
      fail("Should fail with EntityIdException");
    } catch (EntityIdException eie) {
      assertEquals("Malformed hbase Row Key", eie.getMessage());
    }
  }

  @Test
  public void testUnicodeZeroStringFormattedEntityId() {
    final RowKeyFormat2 format = makeRowKeyFormat();
    try {
      makeId(format, "one\u0000two");
      fail("Should fail with EntityIdException");
    } catch (EntityIdException eie) {
      assertEquals("String component cannot contain \u0000", eie.getMessage());
    }

  }

  @Test
  public void testNullFormattedEntityId() {
    final RowKeyFormat2 format = makeRowKeyFormat();
    try {
      makeId(format, new Object[] { null });
      fail("Should fail with EntityIdException");
    } catch (EntityIdException eie) {
      LOG.info("Expected exception: {}", eie.getMessage());
      assertTrue(eie.getMessage().contains(
          "Unexpected null component in kiji row key.Expected at least 1 non-null components"));
    }
  }

  @Test
  public void testNullInputFormattedEntityId() {
    final RowKeyFormat2 format = makeRowKeyFormat();
    try {
      FormattedEntityId.getEntityId(null, format);
      fail("Should fail with NullPointerException");
    } catch (NullPointerException npe) {
      assertEquals(null, npe.getMessage());
    }
  }

  @Test
  public void testEmptyFormattedEntityId() {
    final RowKeyFormat2 format = makeRowKeyFormat();
    try {
      makeId(format); // no component specified at all
      fail("Should fail with EntityIdException");
    } catch (EntityIdException eie) {
      assertEquals("Too few components in kiji Row key", eie.getMessage());
    }
  }

  @Test
  public void testHashedEntityId() {
    final RowKeyFormat2 format = makeHashedRowKeyFormat();

    FormattedEntityId formattedEntityId = makeId(format, "one");
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();

    // since we have suppressed materialization, we dont have valid components anymore.
    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());

    try {
      testEntityId.getComponents();
      fail("Should fail with IllegalStateException");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot retrieve components as materialization is suppressed", ise.getMessage());
    }
  }

  @Test
  public void testHashedEntityIdToShellString() {
    final RowKeyFormat2 format = makeHashedRowKeyFormat();

    final FormattedEntityId formattedEntityId = makeId(format, "one");
    assertEquals("hbase=hex:f9", formattedEntityId.toShellString());
  }

  @Test
  public void testDefaultSaltRowKeyFormat() {
    // SCHEMA-335: This should use a default HashSpec rather than triggering an NPE.
    final RowKeyFormat2 format = makeDefaultSaltRowKeyFormat();

    // Test that the salt field is populated with the default value we expect. It's an incompatible
    // change if this ever changes. In effect, the next few asserts test that the Avro IDL is not
    // incompatibly changed.
    HashSpec spec = format.getSalt();
    assertNotNull("Didn't get a default HashSpec, got null!", spec);
    assertEquals("Default hash spec doesn't have the well-defined default size",
        2, (int) spec.getHashSize());
    assertEquals("Default hash spec doesn't have the well-defined default algorithm",
        HashType.MD5, spec.getHashType());
    assertFalse("Default hash spec should have suppressKeyMaterialization false",
        spec.getSuppressKeyMaterialization());

    // And test that FormattedEntityId itself no longer has theNPE.
    final FormattedEntityId formattedEntityId = makeId(format, "one", 1, 7L);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();

    final FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }
}
