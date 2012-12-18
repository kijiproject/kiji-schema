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

package org.kiji.schema.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestAvroJson {
  private static final Logger LOG = LoggerFactory.getLogger(TestAvroJson.class);

  @Test
  public void testSerializeDeserialize() throws Exception {
    final TableLayoutDesc desc = KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
    final String json = ToJson.toJsonString(desc);
    LOG.info("Serialized JSON:\n{}", json);
    final TableLayoutDesc avro =
        (TableLayoutDesc) FromJson.fromJsonString(json, TableLayoutDesc.SCHEMA$);
    final String jsonAgain = ToJson.toJsonString(avro);
    assertEquals(json, jsonAgain);
  }

}
