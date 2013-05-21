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

package org.kiji.schema.util;

import static org.junit.Assert.assertEquals;

import org.apache.avro.Schema;
import org.junit.Test;

public class TestFromJson {

  @Test
  public void testPrimitivesFromJson() throws Exception {
    assertEquals((Integer) 1, FromJson.fromJsonString("1", Schema.create(Schema.Type.INT)));
    assertEquals((Long) 1L, FromJson.fromJsonString("1", Schema.create(Schema.Type.LONG)));
    assertEquals((Float) 1.0f, FromJson.fromJsonString("1", Schema.create(Schema.Type.FLOAT)));
    assertEquals((Double) 1.0, FromJson.fromJsonString("1", Schema.create(Schema.Type.DOUBLE)));
    assertEquals((Float) 1.0f, FromJson.fromJsonString("1.0", Schema.create(Schema.Type.FLOAT)));
    assertEquals((Double) 1.0, FromJson.fromJsonString("1.0", Schema.create(Schema.Type.DOUBLE)));
    assertEquals("Text", FromJson.fromJsonString("'Text'", Schema.create(Schema.Type.STRING)));
    assertEquals("Text", FromJson.fromJsonString("\"Text\"", Schema.create(Schema.Type.STRING)));
    assertEquals(true, FromJson.fromJsonString("true", Schema.create(Schema.Type.BOOLEAN)));
    assertEquals(false, FromJson.fromJsonString("false", Schema.create(Schema.Type.BOOLEAN)));
  }

}
