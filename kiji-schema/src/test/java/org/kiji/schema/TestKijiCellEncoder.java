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

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.avro.TestRecord;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.layout.CellSpec;

/** Tests for Kiji cell encoders. */
public class TestKijiCellEncoder extends KijiClientTest {
  @Test
  public void testEncodeCounter() throws IOException {
    final KijiCellEncoder encoder =
        DefaultKijiCellEncoderFactory.get().create(CellSpec.newCounter());
    assertArrayEquals(Bytes.toBytes(3181L), encoder.encode(3181));
    assertArrayEquals(Bytes.toBytes(3181L), encoder.encode(3181L));
  }

  @Test
  public void testEncodeAvroInline() throws IOException {
    final CellSpec cellSpec = CellSpec.create()
        .setCellSchema(CellSchema.newBuilder()
            .setStorage(SchemaStorage.FINAL)
            .setType(SchemaType.INLINE)
            .setValue("\"long\"")
            .build());
    final KijiCellEncoder encoder =
        DefaultKijiCellEncoderFactory.get().create(cellSpec);
    // Avro encodes 3181L as bytes [-38, 49]:
    assertArrayEquals(new byte[]{-38, 49}, encoder.encode(3181L));
  }

  @Test
  public void testEncodeAvroClass() throws IOException {
    final CellSpec cellSpec = CellSpec.create()
        .setCellSchema(CellSchema.newBuilder()
            .setStorage(SchemaStorage.FINAL)
            .setType(SchemaType.CLASS)
            .setValue(TestRecord.class.getName())
            .build());
    final KijiCellEncoder encoder =
        DefaultKijiCellEncoderFactory.get().create(cellSpec);
    final TestRecord record = TestRecord.newBuilder()
        .setA("a")  // encodes as [2, 97]
        .setB(1)    // encodes as [2]
        .setC(2)    // encodes as [4]
        .build();
    assertArrayEquals(new byte[]{2, 97, 2, 4}, encoder.encode(record));
  }

  @Test
  public void testEncodeAvroSchemaUID() throws IOException {
    final CellSpec cellSpec = CellSpec.create()
        .setCellSchema(CellSchema.newBuilder()
            .setStorage(SchemaStorage.UID)
            .setType(SchemaType.INLINE)
            .setValue("\"long\"")
            .build())
        .setSchemaTable(getKiji().getSchemaTable());
    final KijiCellEncoder encoder =
        DefaultKijiCellEncoderFactory.get().create(cellSpec);
    // Avro schema "long" has UID #3, and Avro encodes 3181L as bytes [-38, 49]:
    assertArrayEquals(new byte[]{3, -38, 49}, encoder.encode(3181L));
  }

  @Test
  public void testEncodeAvroSchemaHash() throws IOException {
    final CellSpec cellSpec = CellSpec.create()
        .setCellSchema(CellSchema.newBuilder()
            .setStorage(SchemaStorage.HASH)
            .setType(SchemaType.INLINE)
            .setValue("\"long\"")
            .build())
        .setSchemaTable(getKiji().getSchemaTable());
    final KijiCellEncoder encoder =
        DefaultKijiCellEncoderFactory.get().create(cellSpec);
    final byte[] bytes = encoder.encode(3181L);
    assertEquals(16 + 2, bytes.length);
    // Avro encodes 3181L as bytes [-38, 49]:
    assertEquals(-38, bytes[16]);
    assertEquals(49, bytes[17]);
  }
}
