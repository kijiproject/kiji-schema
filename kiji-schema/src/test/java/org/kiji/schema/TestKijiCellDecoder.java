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

import org.apache.avro.Schema;
import org.junit.Test;

import org.kiji.schema.avro.Node;

public class TestKijiCellDecoder extends KijiClientTest {
  @Test
  public void testDecodeSpecific() throws IOException {
    for (KijiCellFormat format : KijiCellFormat.values()) {
      decodeSpecific(format);
    }
  }

  private void decodeSpecific(KijiCellFormat format) throws IOException {
    final Node expected = new Node();
    expected.setWeight(1.0);
    expected.setLabel("foo");
    final byte[] encodedBytes = getCellEncoder()
        .encode(new KijiCell<Node>(Node.SCHEMA$, expected), format);

    final KijiCellDecoderFactory cellDecoderFactory =
        new SpecificCellDecoderFactory(getKiji().getSchemaTable());
    final KijiCellDecoder<Node> cellDecoder =
        cellDecoderFactory.create(Node.class, format);
    final KijiCell<Node> cell = cellDecoder.decode(encodedBytes);
    final Node node = cell.getData();
    assertEquals("foo", node.getLabel().toString());
  }

  @Test
  public void testDecodeGeneric() throws IOException {
    for (KijiCellFormat format : KijiCellFormat.values()) {
      decodeGeneric(format);
    }
  }

  private void decodeGeneric(KijiCellFormat format) throws IOException {
    final byte[] encodedBytes = getCellEncoder()
        .encode(new KijiCell<Integer>(Schema.create(Schema.Type.INT), new Integer(42)), format);

    final KijiCellDecoderFactory cellDecoderFactory =
        new GenericCellDecoderFactory(getKiji().getSchemaTable());
    final KijiCellDecoder<Integer> cellDecoder =
        cellDecoderFactory.create(Schema.create(Schema.Type.INT), format);
    final KijiCell<Integer> cell = cellDecoder.decode(encodedBytes);
    final Integer value = cell.getData();
    assertEquals(42, value.intValue());
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testExceptionWhenPassingTypesToGenericDecoderFactory() throws IOException {
    final KijiCellDecoderFactory cellDecoderFactory =
        new GenericCellDecoderFactory(getKiji().getSchemaTable());
    final KijiCellDecoder<Node> cellDecoder =
        cellDecoderFactory.create(Node.class, KijiCellFormat.HASH);
  }
}
