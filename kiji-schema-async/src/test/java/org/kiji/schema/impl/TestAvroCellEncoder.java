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
package org.kiji.schema.impl;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiEncodingException;
import org.kiji.schema.avro.AvroValidationPolicy;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.CellSpec;

public class TestAvroCellEncoder extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestAvroCellEncoder.class);

  /**
   * Tests that AvroCellEncoder throws an appropriate KijiEncodingException if no Avro writer
   * schema can be inferred for the specified value to encode.
   */
  @Test
  public void testWriterSchemaNotInferrable() throws IOException {
    final Kiji kiji = getKiji();
    final CellSpec cellSpec = CellSpec.create()
        .setCellSchema(CellSchema.newBuilder()
            .setType(SchemaType.AVRO)
            .setAvroValidationPolicy(AvroValidationPolicy.DEVELOPER)
            .build())
        .setSchemaTable(kiji.getSchemaTable());
    final AvroCellEncoder encoder = new AvroCellEncoder(cellSpec);
    try {
      encoder.encode(new Object());
      Assert.fail("AvroCellEncoder.encode() should throw a KijiEncodingException.");
    } catch (KijiEncodingException kee) {
      LOG.info("Expected error: '{}'", kee.getMessage());
      Assert.assertTrue(kee.getMessage().contains("Unable to infer Avro writer schema for value"));
    }
  }

}
