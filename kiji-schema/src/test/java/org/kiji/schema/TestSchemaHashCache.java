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
import static org.junit.Assert.assertFalse;

import org.apache.avro.Schema;
import org.junit.Test;

import org.kiji.schema.KijiSchemaTable.SchemaHashCache;
import org.kiji.schema.util.BytesKey;

/** Tests for SchemaHashCache. */
public class TestSchemaHashCache extends KijiClientTest {
  @Test
  public void testSchemaMD5CacheTwoIdenticalPrimitives() throws Exception {
    final Schema intSchema1 = Schema.create(Schema.Type.INT);
    final Schema intSchema2 = Schema.create(Schema.Type.INT);

    final SchemaHashCache cache = new SchemaHashCache();
    final BytesKey key1 = cache.getHash(intSchema1);
    assertEquals(key1, cache.getHash(intSchema1));
    final BytesKey key2 = cache.getHash(intSchema2);
    assertEquals(key1, key2);
  }

  /** A modified documentation should produce a different hash. */
  @Test
  public void testSchemaMD5CacheTwoIdenticalRecords() throws Exception {
    final String jsonSchema1 =
        "{"
        + "  \"name\": \"Record\","
        + "  \"type\": \"record\","
        + "  \"doc\": \"Documentation\","
        + "  \"fields\": []"
        + "}";
    final Schema schema1 = new Schema.Parser().parse(jsonSchema1);
    final Schema schema1bis = new Schema.Parser().parse(jsonSchema1);

    final Schema schema2 = new Schema.Parser().parse(
        "{"
        + "  \"name\": \"Record\","
        + "  \"type\": \"record\","
        + "  \"doc\": \"Modified documentation\","
        + "  \"fields\": []"
        + "}");

    final SchemaHashCache cache = new SchemaHashCache();
    final BytesKey key1 = cache.getHash(schema1);
    assertEquals(key1, cache.getHash(schema1));

    final BytesKey key1bis = cache.getHash(schema1bis);
    final BytesKey key2 = cache.getHash(schema2);
    assertEquals(key1, key1bis);
    assertFalse(key1.equals(key2));
  }
}
