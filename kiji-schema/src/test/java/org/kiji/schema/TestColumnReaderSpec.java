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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiTableReaderBuilder.OnDecoderCacheMiss;
import org.kiji.schema.avro.EmptyRecord;
import org.kiji.schema.avro.TestRecord1;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestColumnReaderSpec extends KijiClientTest {

  private static final KijiColumnName EMPTY = KijiColumnName.create("family", "empty");

  @Before
  public void setup() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.READER_SCHEMA_TEST))
            .withRow("row")
                .withFamily("family")
                    .withQualifier("empty")
                        .withValue(5, EmptyRecord.newBuilder().build())
        .build();
  }

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    final ColumnReaderSpec spec = ColumnReaderSpec.avroDefaultReaderSchemaGeneric();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(spec);
    final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    final ObjectInputStream ois = new ObjectInputStream(bais);
    final ColumnReaderSpec spec2 = (ColumnReaderSpec) ois.readObject();
    assertEquals(spec, spec2);
  }

  @Test
  public void testColumnOverride() throws IOException {
    final KijiDataRequest normalRequest =
        KijiDataRequest.create(EMPTY.getFamily(), EMPTY.getQualifier());
    final KijiDataRequest overrideRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
        .add(EMPTY, ColumnReaderSpec.avroReaderSchemaSpecific(TestRecord1.class))).build();

    final KijiTable table = getKiji().openTable("table");
    try {
      final EntityId eid = table.getEntityId("row");
      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiRowData normalData = reader.get(eid, normalRequest);
        final EmptyRecord emptyRecord = normalData.getMostRecentValue("family", "empty");

        final KijiRowData overrideData = reader.get(table.getEntityId("row"), overrideRequest);
        final TestRecord1 record1 = overrideData.getMostRecentValue("family", "empty");
        assertEquals(Integer.valueOf(-1), record1.getInteger());
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testFailOnOverride() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
        .add(EMPTY, ColumnReaderSpec.avroReaderSchemaSpecific(TestRecord1.class))).build();

    final KijiTable table = getKiji().openTable("table");
    try {
      final EntityId eid = table.getEntityId("row");
      final KijiTableReader reader = table.getReaderFactory().readerBuilder()
          .withOnDecoderCacheMiss(OnDecoderCacheMiss.FAIL).buildAndOpen();
      try {
        try {
          final KijiRowData data = reader.get(table.getEntityId("row"), request);
          final TestRecord1 record1 = data.getMostRecentValue("family", "empty");
          fail("Should have thrown DecoderNotFoundException");
        } catch (DecoderNotFoundException dnfe) {
          assertTrue(dnfe.getMessage().startsWith(
              "Could not find cell decoder for BoundColumnReaderSpec: "));
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testOverrideAtConstruction() throws IOException {
    final KijiDataRequest normalRequest =
        KijiDataRequest.create(EMPTY.getFamily(), EMPTY.getQualifier());
    final KijiDataRequest overrideRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
        .add(EMPTY, ColumnReaderSpec.avroReaderSchemaSpecific(TestRecord1.class))).build();

    final KijiTable table = getKiji().openTable("table");
    try {
      final EntityId eid = table.getEntityId("row");
      final KijiTableReader reader = table.getReaderFactory().readerBuilder()
          .withOnDecoderCacheMiss(OnDecoderCacheMiss.FAIL)
          .withColumnReaderSpecOverrides(ImmutableMap.of(
              EMPTY, ColumnReaderSpec.avroReaderSchemaSpecific(TestRecord1.class)))
          .buildAndOpen();
      try {
        final KijiRowData normalData = reader.get(eid, normalRequest);
        final TestRecord1 record1 = normalData.getMostRecentValue("family", "empty");
        assertEquals(Integer.valueOf(-1), record1.getInteger());

        final KijiRowData overrideData = reader.get(eid, overrideRequest);
        final TestRecord1 record1b = overrideData.getMostRecentValue("family", "empty");
        assertEquals(Integer.valueOf(-1), record1b.getInteger());
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testAlternative() throws IOException {
    final KijiDataRequest normalRequest =
        KijiDataRequest.create(EMPTY.getFamily(), EMPTY.getQualifier());
    final KijiDataRequest overrideRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .add(EMPTY, ColumnReaderSpec.avroReaderSchemaSpecific(TestRecord1.class))).build();

    final KijiTable table = getKiji().openTable("table");
    try {
      final EntityId eid = table.getEntityId("row");
      final Multimap<KijiColumnName, ColumnReaderSpec> alts = ImmutableSetMultimap.of(
          EMPTY, ColumnReaderSpec.avroReaderSchemaSpecific(TestRecord1.class));
      final KijiTableReader reader = table.getReaderFactory().readerBuilder()
          .withOnDecoderCacheMiss(OnDecoderCacheMiss.FAIL)
          .withColumnReaderSpecAlternatives(alts)
          .buildAndOpen();
      try {
        final KijiRowData normalData = reader.get(eid, normalRequest);
        final EmptyRecord emptyRecord = normalData.getMostRecentValue("family", "empty");

        final KijiRowData overrideData = reader.get(eid, overrideRequest);
        final TestRecord1 record1 = overrideData.getMostRecentValue("family", "empty");
        assertEquals(Integer.valueOf(-1), record1.getInteger());
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}
