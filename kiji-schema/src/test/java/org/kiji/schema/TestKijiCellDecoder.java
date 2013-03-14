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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.Edge;
import org.kiji.schema.avro.Node;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.layout.CellSpec;

/** Tests for Kiji cell decoders. */
public class TestKijiCellDecoder extends KijiClientTest {
  @Test
  public void testDecodeCounter() throws IOException {
    final CellSpec cellSpec = CellSpec.newCounter();
    final KijiCellEncoder encoder = DefaultKijiCellEncoderFactory.get().create(cellSpec);
    final KijiCellDecoder<Long> decoder = SpecificCellDecoderFactory.get().create(cellSpec);
    assertEquals(3181L, (long) decoder.decodeValue(encoder.encode(3181L)));
  }

  private void testDecodeAvroSchema(SchemaStorage storage) throws IOException {
    final CellSpec cellSpec = CellSpec.create()
        .setCellSchema(CellSchema.newBuilder()
            .setType(SchemaType.CLASS)
            .setValue(Node.class.getName())
            .setStorage(storage)
            .build())
        .setSchemaTable(getKiji().getSchemaTable());

    final Node node = Node.newBuilder()
        .setWeight(1.0)
        .setLabel("foo")
        .setAnnotations(Collections.<String, String>emptyMap())
        .setEdges(Collections.<Edge>emptyList())
        .build();

    // Encode the node:
    final KijiCellEncoder encoder = DefaultKijiCellEncoderFactory.get().create(cellSpec);
    final byte[] bytes = encoder.encode(node);

    // Decode as a specific record:
    {
      final KijiCellDecoder<Node> decoder = SpecificCellDecoderFactory.get().create(cellSpec);
      final Node decoded = decoder.decodeValue(bytes);
      assertEquals("foo", decoded.getLabel().toString());
    }

    // Decode as a generic record:
    {
      final KijiCellDecoder<GenericRecord> decoder =
          GenericCellDecoderFactory.get().create(cellSpec);
      final GenericRecord decoded = decoder.decodeValue(bytes);
      assertEquals("foo", decoded.get("label").toString());
    }
  }

  @Test
  public void testDecodeAvroSchemaUID() throws IOException {
    testDecodeAvroSchema(SchemaStorage.UID);
  }

  @Test
  public void testDecodeAvroSchemaHash() throws IOException {
    testDecodeAvroSchema(SchemaStorage.HASH);
  }

  @Test
  public void testDecodeAvroSchemaFinal() throws IOException {
    testDecodeAvroSchema(SchemaStorage.FINAL);
  }
}
