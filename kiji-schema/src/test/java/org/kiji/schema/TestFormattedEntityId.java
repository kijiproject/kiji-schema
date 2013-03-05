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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.HashSpec;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.ResourceUtils;

/** Tests for FormattedEntityId. */
public class TestFormattedEntityId extends KijiClientTest {

  private RowKeyFormat2 makeRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("astring").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeStringRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("astring").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("bstring").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("cstring").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeRowKeyFormatNoNulls() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("astring").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setNullableStartIndex(components.size())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeIntRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder()
            .setHashSize(8).build())
        .setNullableStartIndex(components.size())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeOrderingTestRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("dummy").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("str1").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("str2").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder()
            .setHashSize(2).build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeCompositeHashRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("astring").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setRangeScanStartIndex(2)
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeHashedRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("astring").setType(ComponentType.STRING).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().setSuppressKeyMaterialization(true)
            .setHashSize(components.size()).build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeCompletelyHashedRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("astring").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setRangeScanStartIndex(components.size())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat2 makeSuppressMaterializationTestRKF() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("astring").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().setSuppressKeyMaterialization(true).build())
        .setRangeScanStartIndex(2)
        .setComponents(components)
        .build();

    return format;
  }

  @Test
  public void testFormattedEntityId() {
    RowKeyFormat2 format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new String("one"), actuals.get(0));
    assertEquals(new Integer(1), actuals.get(1));
    assertEquals(new Long(7L), actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testFormattedEntityIdUsingFactory() {
    RowKeyFormat2 format = makeRowKeyFormat();

    EntityIdFactory entityIdFactory = EntityIdFactory.getFactory(format);
    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));

    // passing args as list
    EntityId formattedEntityId = entityIdFactory.getEntityId(inputRowKey);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    EntityId testEntityId = entityIdFactory.getEntityIdFromHBaseRowKey(hbaseRowKey);
    List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new String("one"), actuals.get(0));
    assertEquals(new Integer(1), actuals.get(1));
    assertEquals(new Long(7L), actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());

    // passing args as varargs
    EntityId formattedEntityId2 = entityIdFactory.getEntityId(new String("two"),
        new Integer(2), new Long(72L));
    byte[] hbaseRowKey2 = formattedEntityId2.getHBaseRowKey();
    assertNotNull(hbaseRowKey2);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey2) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    EntityId testEntityId2 = entityIdFactory.getEntityIdFromHBaseRowKey(hbaseRowKey2);
    List<Object> actuals2 = testEntityId2.getComponents();
    assertEquals(format.getComponents().size(), actuals2.size());
    assertEquals(new String("two"), actuals2.get(0));
    assertEquals(new Integer(2), actuals2.get(1));
    assertEquals(new Long(72L), actuals2.get(2));

    assertArrayEquals(formattedEntityId2.getHBaseRowKey(), testEntityId2.getHBaseRowKey());
  }

  @Test
  public void testOtherHashSizes() {
    RowKeyFormat2 format = makeIntRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new Integer(1));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new Integer(1), actuals.get(0));
    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());

  }

  @Test
  public void testFormattedEntityIdWithNull() {
    RowKeyFormat2 format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(null);

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new String("one"), actuals.get(0));
    assertEquals(new Integer(1), actuals.get(1));
    assertNull(actuals.get(2));

    // another way of doing nulls
    List<Object> inputRowKey1 = new ArrayList<Object>();
    inputRowKey1.add(new String("one"));
    inputRowKey1.add(new Integer(1));
    FormattedEntityId formattedEntityId1 = FormattedEntityId.getEntityId(inputRowKey, format);

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), formattedEntityId1.getHBaseRowKey());
  }

  @Test
  public void testFormattedEntityIdOrdering() {
    RowKeyFormat2 rkf = makeOrderingTestRowKeyFormat();

    List<FormattedEntityId> expected = new ArrayList<FormattedEntityId>();

    // We expect this to be the ordering for the keys below.
    // x is the dummy key to make all hashes equal.
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a"), new String("bc")), rkf));
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a0"), new String("aa")), rkf));
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a0"), new String("aa"),
            Integer.valueOf(-1)), rkf));
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a0"), new String("aa"),
            Integer.valueOf(0)), rkf));
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a0"), new String("aa"),
            Integer.valueOf(Integer.MAX_VALUE)), rkf));
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(-1)), rkf));
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(0)), rkf));
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(0), Long.valueOf(Long.MIN_VALUE)), rkf));
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(0), Long.valueOf(0)), rkf));
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(0), Long.valueOf(Long.MAX_VALUE)), rkf));
    expected.add(FormattedEntityId.getEntityId(
        Arrays.asList((Object) new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(Integer.MAX_VALUE)), rkf));

    SortedMap<byte[], FormattedEntityId> hashedOrder =
        new TreeMap<byte[], FormattedEntityId>(new Bytes.ByteArrayComparator());
    for (FormattedEntityId fid: expected) {
      hashedOrder.put(fid.getHBaseRowKey(), fid);
    }

    int i = 0;
    for (byte[] key: hashedOrder.keySet()) {
      assertEquals(expected.get(i), hashedOrder.get(key));
      for (byte b: key) {
        System.out.format("%x ", b);
      }
      System.out.format("\n");
      i += 1;
    }
  }

  @Test
  public void testCompositeHashEntityId() {
    RowKeyFormat2 format = makeCompositeHashRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new String("one"), actuals.get(0));
    assertEquals(new Integer(1), actuals.get(1));
    assertEquals(new Long(7L), actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testEmptyStringFormattedEntityId() {
    RowKeyFormat2 format = makeStringRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String(""));
    inputRowKey.add(new String(""));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new String(""), actuals.get(0));
    assertEquals(new String(""), actuals.get(1));
    assertEquals(null, actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());

  }

  @Test
  public void testNullValueInHash() {
    RowKeyFormat2 format = makeCompletelyHashedRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    // set one component as null explicitly, do not include the next one at all
    inputRowKey.add(null);

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new String("one"), actuals.get(0));
    assertNull(actuals.get(1));
    assertNull(actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testExplicitNullValueInHash() {
    RowKeyFormat2 format = makeCompletelyHashedRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(null);
    inputRowKey.add(null);

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new String("one"), actuals.get(0));
    assertNull(actuals.get(1));
    assertNull(actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testImplicitNullValueInHash() {
    RowKeyFormat2 format = makeCompletelyHashedRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    // implicit nulls for remaining components
    inputRowKey.add(new String("one"));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    List<Object> actuals = testEntityId.getComponents();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new String("one"), actuals.get(0));
    assertNull(actuals.get(1));
    assertNull(actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testSuppressMaterialization() {
    RowKeyFormat2 format = makeSuppressMaterializationTestRKF();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testSuppressKeyMaterializationInKiji() throws IOException {

    Kiji kiji = getKiji().retain();
    kiji.createTable("table",
        KijiTableLayouts.getTableLayout(KijiTableLayouts.HASHED_FORMATTED_RKF));
    KijiTable table = kiji.openTable("table");
    KijiTableWriter kijiTableWriter = table.openTableWriter();
    KijiTableReader kijiTableReader = table.openTableReader();
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(1).add("family", "column");
    KijiDataRequest dataRequest = builder.build();

    kijiTableWriter.put(table.getEntityId(new String("x")),
        "family", "column", "1");

    kijiTableWriter.flush();

    KijiRowData result = kijiTableReader.get(table.getEntityId(new String("x")), dataRequest);
    assertTrue(result.containsColumn("family", "column"));

    String res = result.getMostRecentValue("family", "column").toString();
    assertEquals("1", res);
    assertArrayEquals(table.getEntityId(new String("x")).getHBaseRowKey(),
        result.getEntityId().getHBaseRowKey());

    ResourceUtils.closeOrLog(kijiTableReader);
    ResourceUtils.closeOrLog(kijiTableWriter);
    ResourceUtils.releaseOrLog(table);
    ResourceUtils.releaseOrLog(kiji);

    try {
      result.getEntityId().getComponents();
    } catch (IllegalStateException ise) {
      System.out.println("Expected exception " + ise);
    }
  }

  @Test(expected=IllegalStateException.class)
  public void testSuppressMaterializationComponentsException() {
    RowKeyFormat2 format = makeSuppressMaterializationTestRKF();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    formattedEntityId.getComponents();
  }

  @Test(expected=IllegalStateException.class)
  public void testSuppressMaterializationComponentsByIndexException() {
    RowKeyFormat2 format = makeSuppressMaterializationTestRKF();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    formattedEntityId.getComponentByIndex(0);
  }

  @Test(expected=IllegalStateException.class)
  public void testSuppressMaterializationComponentsExceptionFromHbaseKey() {
    RowKeyFormat2 format = makeSuppressMaterializationTestRKF();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);

    testEntityId.getComponents();
  }

  @Test(expected=IllegalStateException.class)
  public void testSuppressMaterializationComponentsByIndexExceptionFromHbaseKey() {
    RowKeyFormat2 format = makeSuppressMaterializationTestRKF();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);

    testEntityId.getComponentByIndex(0);
  }

  @Test(expected=EntityIdException.class)
  public void testBadNullFormattedEntityId() {
    RowKeyFormat2 format = makeRowKeyFormat();

    // no nonnull values allowed after null
    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(null);
    inputRowKey.add(new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
  }

  @Test(expected=EntityIdException.class)
  public void testTooManyComponentsFormattedEntityId() {
    RowKeyFormat2 format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));
    inputRowKey.add(null);

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
  }

  @Test(expected=EntityIdException.class)
  public void testWrongComponentTypeFormattedEntityId() {
    RowKeyFormat2 format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    // should be integer.
    inputRowKey.add(new Long(1L));
    inputRowKey.add(new Long(7));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
  }

  @Test(expected=EntityIdException.class)
  public void testUnexpectedNullFormattedEntityId() {
    RowKeyFormat2 format = makeRowKeyFormatNoNulls();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(null);

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
  }

  @Test(expected=EntityIdException.class)
  public void testTooFewComponentsFormattedEntityId() {
    RowKeyFormat2 format = makeRowKeyFormatNoNulls();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
  }

  @Test(expected=EntityIdException.class)
  public void testBadHbaseKey() {
    RowKeyFormat2 format = makeIntRowKeyFormat();
    byte[] hbkey = new byte[]{(byte) 0x01, (byte) 0x02, (byte) 0x042, (byte) 0x01, (byte) 0x02,
        (byte) 0x042, (byte) 0x01, (byte) 0x02, (byte) 0x042, };

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbkey, format);
  }

  @Test(expected=EntityIdException.class)
  public void testUnicodeZeroStringFormattedEntityId() {
    RowKeyFormat2 format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one\u0000two"));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
  }

  @Test(expected=EntityIdException.class)
  public void testNullFormattedEntityId() {
    RowKeyFormat2 format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(null);

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
  }

  @Test(expected=NullPointerException.class)
  public void testNullInputFormattedEntityId() {
    RowKeyFormat2 format = makeRowKeyFormat();

    List<Object> inputRowKey = null;

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
  }

  @Test(expected=EntityIdException.class)
  public void testEmptyFormattedEntityId() {
    RowKeyFormat2 format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
  }

  @Test(expected=IllegalStateException.class)
  public void testHashedEntityId() {
    RowKeyFormat2 format = makeHashedRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));

    FormattedEntityId formattedEntityId = FormattedEntityId.getEntityId(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    // since we have suppressed materialization, we dont have valid components anymore.
    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());

    List<Object> actuals = testEntityId.getComponents();
  }
}
