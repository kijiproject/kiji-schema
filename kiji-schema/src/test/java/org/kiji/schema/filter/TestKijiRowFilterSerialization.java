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

package org.kiji.schema.filter;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import org.kiji.schema.DecodedCell;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.HashSpec;
import org.kiji.schema.avro.HashType;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.TestComplexRecord;
import org.kiji.schema.avro.TestRecord;

/** Tests the serialization and deserialization of KijiRowFilters. */
public class TestKijiRowFilterSerialization {
  @Test
  public void testStripValueRowFilter() throws Exception {
    runTest(new StripValueRowFilter());
  }

  @Test
  public void testHasColumnDataRowFilter() throws Exception {
    runTest(new HasColumnDataRowFilter("family", "qualifier"));
  }

  @Test
  public void testColumnValueEqualsRowFilter() throws Exception {
    TestRecord record = createTestRecord();
    runTest(new ColumnValueEqualsRowFilter("family", "qualifier",
        new DecodedCell<Object>(record.getSchema(), record)));
  }

  @Test
  public void testAndRowFilter() throws Exception {
    TestRecord record = createTestRecord();
    runTest(Filters.and(
        new HasColumnDataRowFilter("fA", "qA"),
        new HasColumnDataRowFilter("fB", "qB"),
        new ColumnValueEqualsRowFilter("fC", "qC",
            new DecodedCell<Object>(record.getSchema(), record))));
  }

  @Test
  public void testOrRowFilter() throws Exception {
    TestRecord record = createTestRecord();
    runTest(Filters.or(
        new HasColumnDataRowFilter("fA", "qA"),
        new HasColumnDataRowFilter("fB", "qB"),
        new ColumnValueEqualsRowFilter("fC", "qC",
            new DecodedCell<Object>(record.getSchema(), record))));
  }

  @Test
  public void testComplexRecordInColumnValueEqualsRowFilter() throws Exception {
    byte[] bytes = new byte[100];
    (new Random()).nextBytes(bytes);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    TestComplexRecord record = TestComplexRecord.newBuilder()
        .setA("a string value")
        .setB(10)
        .setD(buffer)
        .setE(null)
        .setF(true)
        .setG("another string")
        .build();
    runTest(new ColumnValueEqualsRowFilter("family", "qualifier",
        new DecodedCell<Object>(record.getSchema(), record)));
  }

  @Test
  public void testFormattedEntityIdRowFilter() throws Exception {
    runTest(new FormattedEntityIdRowFilter(
        RowKeyFormat2.newBuilder()
            .setEncoding(RowKeyEncoding.FORMATTED)
            .setSalt(new HashSpec(HashType.MD5, 1, false))
            .setComponents(ImmutableList.of(
                new RowKeyComponent("a", ComponentType.INTEGER),
                new RowKeyComponent("b", ComponentType.LONG),
                new RowKeyComponent("c", ComponentType.STRING)))
            .build(),
        100, null, "value"));
  }

  private void runTest(KijiRowFilter original) throws Exception {
    JsonNode serialized = original.toJson();
    KijiRowFilter deserialized = KijiRowFilter.toFilter(serialized.toString());
    JsonNode reserialized = deserialized.toJson();
    assertEquals(serialized, reserialized);
  }

  private TestRecord createTestRecord() {
    TestRecord record = TestRecord.newBuilder()
        .setA("a string value")
        .setB(10)
        .setC(100)
        .build();
    return record;
  }
}
