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

package org.kiji.schema.mapreduce;

import static org.kiji.schema.util.WritableTestUtil.assertWritable;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.impl.HBaseEntityId;

public class TestKijiDelete {

  @Test
  public void testWritableSerialization() throws Exception {
    final EntityId entityId = new HBaseEntityId(Bytes.toBytes("foo"));
    final String family = "family";
    final String qualifier = "qualifier";
    final Long timestamp = Long.MAX_VALUE;
    final KijiDelete.KijiDeleteScope scope = KijiDelete.KijiDeleteScope.MULTIPLE_VERSIONS;

    KijiDelete expected = new KijiDelete(entityId, family, qualifier, timestamp, scope);
    assertWritable(expected, KijiDelete.class);

    expected = new KijiDelete(entityId, timestamp);
    assertWritable(expected, KijiDelete.class);

    expected = new KijiDelete(entityId, family, timestamp, scope);
    assertWritable(expected, KijiDelete.class);

    expected = new KijiDelete(entityId);
    assertWritable(expected, KijiDelete.class);

    expected = new KijiDelete(entityId, family, scope);
    assertWritable(expected, KijiDelete.class);

    expected = new KijiDelete(entityId, family, qualifier, scope);
    assertWritable(expected, KijiDelete.class);
  }
}
