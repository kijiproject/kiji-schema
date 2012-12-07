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

package org.kiji.schema.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.UnsupportedEncodingException;
import java.util.*;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.avro.*;

/** Tests for FormattedEntityId. */
public class TestFormattedEntityId {

  private RowKeyFormat makeRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("astring").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    RowKeyFormat format = RowKeyFormat.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat makeRowKeyFormatNoNulls() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("astring").setType(ComponentType.STRING).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());
    components.add(RowKeyComponent.newBuilder()
        .setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    RowKeyFormat format = RowKeyFormat.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().build())
        .setNullableIndex(components.size())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat makeIntRowKeyFormat() {
    // components of the row key
    ArrayList<RowKeyComponent> components = new ArrayList<RowKeyComponent>();
    components.add(RowKeyComponent.newBuilder()
        .setName("anint").setType(ComponentType.INTEGER).build());

    // build the row key format
    RowKeyFormat format = RowKeyFormat.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder()
            .setHashSize(2).build())
        .setNullableIndex(components.size())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat makeOrderingTestRowKeyFormat() {
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
    RowKeyFormat format = RowKeyFormat.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder()
            .setHashSize(2).build())
        .setComponents(components)
        .build();

    return format;
  }

  @Test
  public void testFormattedEntityId() {
    RowKeyFormat format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.fromKijiRowKey(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }
    System.out.format("\n");

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    List<Object> actuals = testEntityId.getKijiRowKey();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new String("one"), actuals.get(0));
    assertEquals(new Integer(1), actuals.get(1));
    assertEquals(new Long(7L), actuals.get(2));

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), testEntityId.getHBaseRowKey());
  }

  @Test
  public void testFormattedEntityIdWithNull() {
    RowKeyFormat format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(null);

    FormattedEntityId formattedEntityId = FormattedEntityId.fromKijiRowKey(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    List<Object> actuals = testEntityId.getKijiRowKey();
    assertEquals(format.getComponents().size(), actuals.size());
    assertEquals(new String("one"), actuals.get(0));
    assertEquals(new Integer(1), actuals.get(1));
    assertEquals(null, actuals.get(2));

    // another way of doing nulls
    List<Object> inputRowKey1 = new ArrayList<Object>();
    inputRowKey1.add(new String("one"));
    inputRowKey1.add(new Integer(1));
    FormattedEntityId formattedEntityId1 = FormattedEntityId.fromKijiRowKey(inputRowKey, format);

    assertArrayEquals(formattedEntityId.getHBaseRowKey(), formattedEntityId1.getHBaseRowKey());
  }

  @Test
  public void testFormattedEntityIdOrdering() {
    RowKeyFormat rkf = makeOrderingTestRowKeyFormat();

    List<FormattedEntityId> expected = new ArrayList<FormattedEntityId>();

    // We expect this to be the ordering for the keys below.
    // x is the dummy key to make all hashes equal.
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object) new String("x"), new String("a"), new String("bc")), rkf));
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object)new String("x"), new String("a0"), new String("aa")), rkf));
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object)new String("x"), new String("a0"), new String("aa"),
            Integer.valueOf(-1)), rkf));
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object)new String("x"), new String("a0"), new String("aa"),
            Integer.valueOf(0)), rkf));
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object)new String("x"), new String("a0"), new String("aa"),
            Integer.valueOf(Integer.MAX_VALUE)), rkf));
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object)new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(-1)), rkf));
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object)new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(0)), rkf));
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object)new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(0), Long.valueOf(Long.MIN_VALUE)), rkf));
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object)new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(0), Long.valueOf(0)), rkf));
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object)new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(0), Long.valueOf(Long.MAX_VALUE)), rkf));
    expected.add(FormattedEntityId.fromKijiRowKey(
        Arrays.asList((Object)new String("x"), new String("a0"), new String("aa0"),
            Integer.valueOf(Integer.MAX_VALUE)), rkf));

    SortedMap<byte[], FormattedEntityId> hashedOrder = new TreeMap(new Bytes.ByteArrayComparator());
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

  @Test(expected = EntityIdException.class)
  public void testBadNullFormattedEntityId() {
    RowKeyFormat format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(null);
    inputRowKey.add(new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.fromKijiRowKey(inputRowKey, format);
  }

  @Test(expected = EntityIdException.class)
  public void testTooManyComponentsFormattedEntityId() {
    RowKeyFormat format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Integer(1));
    inputRowKey.add(new Long(7L));
    inputRowKey.add(null);

    FormattedEntityId formattedEntityId = FormattedEntityId.fromKijiRowKey(inputRowKey, format);
  }

  @Test(expected = EntityIdException.class)
  public void testWrongComponentTypeFormattedEntityId() {
    RowKeyFormat format = makeRowKeyFormat();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(new Long(1L));
    inputRowKey.add(new Long(7));

    FormattedEntityId formattedEntityId = FormattedEntityId.fromKijiRowKey(inputRowKey, format);
  }

  @Test(expected = EntityIdException.class)
  public void testUnexpectedNullFormattedEntityId() {
    RowKeyFormat format = makeRowKeyFormatNoNulls();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));
    inputRowKey.add(null);

    FormattedEntityId formattedEntityId = FormattedEntityId.fromKijiRowKey(inputRowKey, format);
  }

  @Test(expected = EntityIdException.class)
  public void testTooFewComponentsFormattedEntityId() {
    RowKeyFormat format = makeRowKeyFormatNoNulls();

    List<Object> inputRowKey = new ArrayList<Object>();
    inputRowKey.add(new String("one"));

    FormattedEntityId formattedEntityId = FormattedEntityId.fromKijiRowKey(inputRowKey, format);
  }

  @Test(expected = EntityIdException.class)
  public void testBadHbaseKey(){
    RowKeyFormat format = makeIntRowKeyFormat();
    byte[] hbkey =new byte[]{(byte)0x01, (byte)0x02, (byte)0x042};

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbkey, format);
  }

}
