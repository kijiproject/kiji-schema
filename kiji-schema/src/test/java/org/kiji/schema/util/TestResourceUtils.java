/**
 * (c) Copyright 2014 WibiData, Inc.
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

import java.io.Closeable;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReaderPool;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.ResourceUtils.CompoundException;
import org.kiji.schema.util.ResourceUtils.DoAndClose;
import org.kiji.schema.util.ResourceUtils.DoAndRelease;
import org.kiji.schema.util.ResourceUtils.WithKiji;
import org.kiji.schema.util.ResourceUtils.WithKijiTable;
import org.kiji.schema.util.ResourceUtils.WithKijiTableReader;
import org.kiji.schema.util.ResourceUtils.WithKijiTableWriter;

public class TestResourceUtils extends KijiClientTest {
  private static final String TABLE_NAME = "row_data_test_table";
  private static final KijiDataRequest FAMILY_QUAL0_R = KijiDataRequest.create("family", "qual0");

  private KijiTable mTable = null;

  @Before
  public void setupTestResourceUtils() throws IOException {
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));
    new InstanceBuilder(getKiji())
        .withTable("row_data_test_table", layout)
            .withRow("foo")
                .withFamily("family")
                    .withQualifier("qual0").withValue(5L, "foo-val")
                    .withQualifier("qual1").withValue(5L, "foo-val")
                    .withQualifier("qual2").withValue(5L, "foo@val.com")
                .withFamily("map")
                    .withQualifier("qualifier").withValue(5L, 1)
                    .withQualifier("qualifier1").withValue(5L, 2)
            .withRow("bar")
                .withFamily("family")
                    .withQualifier("qual0").withValue(5L, "bar-val")
                    .withQualifier("qual2").withValue(5L, "bar@val.com")
        .build();
    mTable = getKiji().openTable(TABLE_NAME);
  }

  @After
  public void cleanupTestResourceUtils() throws IOException {
    mTable.release();
  }

  @Test
  public void testWithKijiTableReader() throws Exception {
    Assert.assertEquals("foo-val", new WithKijiTableReader<String>(mTable.getURI()) {
      @Override
      public String run(final KijiTableReader kijiTableReader) throws Exception {
        return kijiTableReader.get(
            mTable.getEntityId("foo"),
            FAMILY_QUAL0_R
        ).getMostRecentValue("family", "qual0").toString();
      }
    }.eval());

    Assert.assertEquals("foo-val", new WithKijiTableReader<String>(mTable) {
      @Override
      public String run(final KijiTableReader kijiTableReader) throws Exception {
        return kijiTableReader.get(
            mTable.getEntityId("foo"),
            FAMILY_QUAL0_R
        ).getMostRecentValue("family", "qual0").toString();
      }
    }.eval());

    final KijiTableReaderPool pool = KijiTableReaderPool.Builder.create()
        .withReaderFactory(mTable.getReaderFactory())
        .build();
    try {
      Assert.assertEquals("foo-val", new WithKijiTableReader<String>(pool) {
        @Override
        public String run(final KijiTableReader kijiTableReader) throws Exception {
          return kijiTableReader.get(
              mTable.getEntityId("foo"),
              FAMILY_QUAL0_R
          ).getMostRecentValue("family", "qual0").toString();
        }
      }.eval());
    } finally {
      pool.close();
    }
  }

  @Test
  public void testWithKijiTableWriter() throws Exception {
    final String expected = "expected";
    Assert.assertEquals(expected, new WithKijiTableWriter<String>(mTable.getURI()) {
      @Override
      public String run(final KijiTableWriter kijiTableWriter) throws Exception {
        kijiTableWriter.put(mTable.getEntityId("bar"), "family", "qual0", expected);
        final KijiTableReader reader = mTable.openTableReader();
        try {
          return reader.get(
              mTable.getEntityId("bar"),
              FAMILY_QUAL0_R
          ).getMostRecentValue("family", "qual0").toString();
        } finally {
          reader.close();
        }
      }
    }.eval());

    Assert.assertEquals(expected, new WithKijiTableWriter<String>(mTable) {
      @Override
      public String run(final KijiTableWriter kijiTableWriter) throws Exception {
        kijiTableWriter.put(mTable.getEntityId("bar"), "family", "qual0", expected);
        final KijiTableReader reader = mTable.openTableReader();
        try {
          return reader.get(
              mTable.getEntityId("bar"),
              FAMILY_QUAL0_R
          ).getMostRecentValue("family", "qual0").toString();
        } finally {
          reader.close();
        }
      }
    }.eval());
  }

  @Test
  public void testWithKijiTable() throws Exception {
    Assert.assertEquals(TABLE_NAME, new WithKijiTable<String>(mTable.getURI()) {
      @Override
      public String run(final KijiTable kijiTable) throws Exception {
        return kijiTable.getName();
      }
    }.eval());

    Assert.assertEquals(TABLE_NAME, new WithKijiTable<String>(getKiji(), TABLE_NAME) {
      @Override
      public String run(final KijiTable kijiTable) throws Exception {
        return kijiTable.getName();
      }
    }.eval());
  }

  @Test
  public void testWithKiji() throws Exception {
    Assert.assertEquals(getKiji().getURI(), new WithKiji<KijiURI>(getKiji().getURI()) {
      @Override
      public KijiURI run(final Kiji kiji) throws Exception {
        return kiji.getURI();
      }
    }.eval());
  }

  @Test
  public void testDoAndClose() throws Exception {
    Assert.assertEquals("foo-val", new DoAndClose<KijiTableReader, String>() {
      @Override
      public KijiTableReader openResource() throws Exception {
        return mTable.openTableReader();
      }

      @Override
      public String run(final KijiTableReader kijiTableReader) throws Exception {
        try {
          return kijiTableReader.get(
              mTable.getEntityId("foo"),
              FAMILY_QUAL0_R
          ).getMostRecentValue("family", "qual0").toString();
        } catch (IOException e) {
          throw new KijiIOException(e);
        }
      }
    }.eval());
  }

  @Test
  public void testDoAndRelease() throws Exception {
    Assert.assertEquals(TABLE_NAME, new DoAndRelease<KijiTable, String>() {
      @Override
      public KijiTable openResource() throws Exception {
        return getKiji().openTable(TABLE_NAME);
      }

      @Override
      public String run(final KijiTable kijiTable) throws Exception {
        return kijiTable.getName();
      }
    }.eval());
  }

  private static final class BrokenCloseable implements Closeable {
    @Override
    public void close() throws IOException {
      throw new IOException("close");
    }
  }

  @Test
  public void testExceptions() throws Exception {
    try {
      new DoAndClose<BrokenCloseable, String>() {

        @Override
        protected BrokenCloseable openResource() throws Exception {
          return new BrokenCloseable();
        }

        @Override
        protected String run(final BrokenCloseable brokenCloseable) throws Exception {
          throw new IOException("run");
        }
      }.eval();
    } catch (CompoundException ce) {
      Assert.assertEquals("Exception was throw while cleaning up resources after another exception "
          + "was thrown.: first exception: run second exception: close", ce.getMessage());
    }
  }
}
