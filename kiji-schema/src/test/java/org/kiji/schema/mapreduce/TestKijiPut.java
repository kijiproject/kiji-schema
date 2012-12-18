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

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.impl.HBaseEntityId;

public class TestKijiPut {
  private final EntityId mEntityId = new HBaseEntityId(Bytes.toBytes("foo"));
  private final String mFamily = "family";
  private final String mQualifier = "qualifier";
  private final long mTimestamp = Long.MAX_VALUE;
  private final KijiCell<?> mCell =
      new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), new Utf8("bar"));

  @Test
  public void testSerializationWithoutTimestamp() throws Exception {
    final KijiPut put = new KijiPut(mEntityId, mFamily, mQualifier, mCell);

    assertWritable(put, KijiPut.class);
  }

  @Test
  public void testSerializationWithTimestamp() throws Exception {
    final KijiPut put = new KijiPut(mEntityId, mFamily, mQualifier, mTimestamp, mCell);

    assertWritable(put, KijiPut.class);
  }
}
