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

package org.kiji.schema.layout;

import com.google.common.collect.ImmutableBiMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.easymock.EasyMock;

import org.kiji.schema.HBaseColumnName;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * A ColumnNameTranslator implementation that can be used in
 * tests. Instead of doing real name translation using a table's
 * layout, it acts as if Kiji and HBase column names are identical.
 */
public class IdentityColumnNameTranslator extends ColumnNameTranslator {
  /** Constructs an identity column name translator that can be used in tests. */
  public IdentityColumnNameTranslator() {
    super(createMockTableLayout());
  }

  /**
   * Constructs a mock table layout.
   *
   * @return A mock table layout.
   */
  private static KijiTableLayout createMockTableLayout() {
    KijiTableLayout mock = EasyMock.createMock(KijiTableLayout.class);
    EasyMock.expect(mock.getLocalityGroupIdNameMap())
        .andReturn(new ImmutableBiMap.Builder<ColumnId, String>().build());
    EasyMock.replay(mock);
    return mock;
  }

  @Override
  public KijiColumnName toKijiColumnName(HBaseColumnName hbaseColumnName) {
    return new KijiColumnName(
        hbaseColumnName.getFamilyAsString(), hbaseColumnName.getQualifierAsString());
  }

  @Override
  public HBaseColumnName toHBaseColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException {
    return new HBaseColumnName(Bytes.toBytes(kijiColumnName.getFamily()),
        Bytes.toBytes(kijiColumnName.getQualifier()));
  }

  @Override
  public KijiTableLayout getTableLayout() {
    throw new UnsupportedOperationException();
  }
}
