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

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import org.kiji.schema.avro.Node;

/**
 * Test KijiCellDecoderFactory implementations (generic and specific).
 */
public class TestKijiCellDecoderFactory extends KijiClientTest {
  @Test
  public void testSpecificFactory() throws IOException {
    final Node expected = new Node();
    expected.setWeight(1.0);
    expected.setLabel("foo");
    final byte[] encodedBytes = getCellEncoder()
        .encode(new KijiCell<Node>(Node.SCHEMA$, expected), KijiCellFormat.HASH);

    final KijiCellDecoderFactory cellDecoderFactory =
        new SpecificCellDecoderFactory(getKiji().getSchemaTable());
    final KijiCellDecoder<Node> cellDecoder =
        cellDecoderFactory.create(Node.class, KijiCellFormat.HASH);
    final KijiCell<Node> cell = cellDecoder.decode(encodedBytes);
    final Node node = cell.getData();
    assertEquals("foo", node.getLabel().toString());
  }

  @Test
  public void testDefaultFactory() throws IOException {
    // KijiCellDecoderFactory.getImpl() should return a specific factory.
    final Node expected = new Node();
    expected.setWeight(1.0);
    expected.setLabel("foo");
    final byte[] encodedBytes = getCellEncoder()
        .encode(new KijiCell<Node>(Node.SCHEMA$, expected), KijiCellFormat.HASH);

    final KijiCellDecoderFactory cellDecoderFactory =
        new SpecificCellDecoderFactory(getKiji().getSchemaTable());
    final KijiCellDecoder<Node> cellDecoder =
        cellDecoderFactory.create(Node.class, KijiCellFormat.HASH);
    final KijiCell<Node> cell = cellDecoder.decode(encodedBytes);
    final Node node = cell.getData();
    assertEquals("foo", node.getLabel().toString());
  }

  @Test
  public void testGenericFactory() throws IOException {
    final Node expected = new Node();
    expected.setWeight(1.0);
    expected.setLabel("foo");
    final byte[] encodedBytes = getCellEncoder()
        .encode(new KijiCell<Node>(Node.SCHEMA$, expected), KijiCellFormat.HASH);

    final KijiCellDecoderFactory cellDecoderFactory =
        new GenericCellDecoderFactory(getKiji().getSchemaTable());
    final KijiCellDecoder<GenericRecord> cellDecoder =
        cellDecoderFactory.create(Node.SCHEMA$, KijiCellFormat.HASH);
    final KijiCell<GenericRecord> cell = cellDecoder.decode(encodedBytes);
    final GenericRecord node = cell.getData();
    assertEquals("foo", node.get("label").toString());

  }
}
