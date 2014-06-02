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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.filter.KijiRowFilterApplicator;
import org.kiji.schema.filter.KijiRowFilterDeserializer;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.hbase.HBaseDataRequestAdapter;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.ScanEquals;

public class TestKijiRowFilterApplicator extends KijiClientTest {
  /** The layout of our test table. */
  private KijiTableLayout mTableLayout;

  /** Translates kiji column names into hbase byte arrays. */
  private HBaseColumnNameTranslator mColumnNameTranslator;

  /** Encodes kiji cells into HBase cells. */
  private KijiCellEncoder mCellEncoder;

  /** The filter returned by the MyKijiRowFilter.toHBaseFilter(). */
  private Filter mHBaseFilter;

  @Before
  public void setupTests() throws IOException {
    mTableLayout =
        KijiTableLayouts.getTableLayout(KijiTableLayouts.SIMPLE_UPDATE_NEW_COLUMN);
    getKiji().createTable(mTableLayout.getDesc());

    mColumnNameTranslator = HBaseColumnNameTranslator.from(mTableLayout);
    final CellSpec cellSpec = mTableLayout.getCellSpec(KijiColumnName.create("family", "new"))
        .setSchemaTable(getKiji().getSchemaTable());
    mCellEncoder = DefaultKijiCellEncoderFactory.get().create(cellSpec);

    mHBaseFilter = new SingleColumnValueFilter(
        Bytes.toBytes("family"), Bytes.toBytes("new"), CompareOp.NO_OP, new byte[0]);
  }

  /**
   * A dumb kiji row filter that doesn't return anything useful, but makes sure that we
   * can use the context object to translate between kiji objects and their hbase counterparts.
   */
  public class MyKijiRowFilter extends KijiRowFilter implements KijiRowFilterDeserializer {
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("family", "new");
    }

    @Override
    public Filter toHBaseFilter(Context context) throws IOException {
      // Make sure we can translate correctly between kiji objects and their HBase counterparts.
      assertArrayEquals("Row key not translated correctly by KijiRowFilter.Context",
          Bytes.toBytes("foo"), context.getHBaseRowKey("foo"));

      final KijiColumnName column = KijiColumnName.create("family", "new");
      final HBaseColumnName hbaseColumn = context.getHBaseColumnName(column);
      final HBaseColumnName expected = mColumnNameTranslator.toHBaseColumnName(column);
      assertArrayEquals("Family name not translated correctly by KijiRowFilter.Context",
          expected.getFamily(), hbaseColumn.getFamily());
      assertArrayEquals("Qualifier name not translated correctly by KijiRowFilter.Context",
          expected.getQualifier(), hbaseColumn.getQualifier());
      final DecodedCell<Integer> kijiCell =
          new DecodedCell<Integer>(Schema.create(Schema.Type.INT), Integer.valueOf(42));
      assertArrayEquals("Cell value not translated correctly by KijiRowFilter.Context",
          mCellEncoder.encode(kijiCell),
          context.getHBaseCellValue(column, kijiCell));

      return mHBaseFilter;
    }

    @Override
    protected JsonNode toJsonNode() {
      return JsonNodeFactory.instance.nullNode();
    }

    @Override
    protected Class<? extends KijiRowFilterDeserializer> getDeserializerClass() {
      return getClass();
    }

    @Override
    public KijiRowFilter createFromJson(JsonNode root) {
      return this;
    }
  }

  @Test
  public void testApplyToScan() throws Exception {
    // Initialize a scan object with some requested data.
    final KijiDataRequest priorDataRequest = KijiDataRequest.create("family", "column");
    final Scan actualScan =
        new HBaseDataRequestAdapter(priorDataRequest, mColumnNameTranslator).toScan(mTableLayout);

    // Construct a row filter and apply it to the existing scan.
    final KijiRowFilter rowFilter = new MyKijiRowFilter();
    final KijiRowFilterApplicator applicator =
        KijiRowFilterApplicator.create(rowFilter, mTableLayout, getKiji().getSchemaTable());
    applicator.applyTo(actualScan);

    // After filter application, expect the scan to also have the column requested by the filter.
    final Scan expectedScan =
        new HBaseDataRequestAdapter(
            priorDataRequest.merge(rowFilter.getDataRequest()), mColumnNameTranslator)
            .toScan(mTableLayout);
    expectedScan.setFilter(mHBaseFilter);
    assertEquals(expectedScan.toString(), actualScan.toString());
    assertTrue(new ScanEquals(expectedScan).matches(actualScan));
  }
}
