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
package org.kiji.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiTableReaderPool.Builder.WhenExhaustedAction;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestKijiTableReaderPool {

  private static final String FOO_NAME = "foo-name";
  private static final String FOO_EMAIL = "foo@email.com";
  private static final KijiDataRequest INFO_NAME = KijiDataRequest.create("info", "name");

  private Kiji mKiji;
  private KijiTable mTable;
  private EntityId mEID;

  @Before
  public void setup() throws IOException {
    mKiji = new InstanceBuilder()
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.USER_TABLE))
            .withRow("foo")
                .withFamily("info")
                    .withQualifier("name")
                        .withValue(1, FOO_NAME)
                    .withQualifier("email")
                        .withValue(1, FOO_EMAIL)
        .build();
    mTable = mKiji.openTable("user");
    mEID = mTable.getEntityId("foo");
  }

  @After
  public void cleanup() throws IOException {
    mTable.release();
    mKiji.release();
  }

  @Test
  public void testSimpleRead() throws Exception {
    final KijiTableReaderPool pool = KijiTableReaderPool.Builder.create()
        .withReaderFactory(mTable.getReaderFactory())
        .build();
    try {
      final KijiTableReader reader = pool.borrowObject();
      try {
        assertEquals(
            FOO_NAME, reader.get(mEID, INFO_NAME).getMostRecentValue("info", "name").toString());
      } finally {
        reader.close();
      }
    } finally {
      pool.close();
    }
  }

  @Test
  public void testFullPool() throws Exception {
    final KijiTableReaderPool pool = KijiTableReaderPool.Builder.create()
        .withReaderFactory(mTable.getReaderFactory())
        .withMaxActive(1)
        .withExhaustedAction(WhenExhaustedAction.FAIL)
        .build();
    try {
      final KijiTableReader reader = pool.borrowObject();
      try {
        final KijiTableReader reader2 = pool.borrowObject();
        fail("getReader() should have thrown NoSuchElementException");
      } catch (NoSuchElementException nsee) {
        assertTrue(nsee.getMessage().equals("Pool exhausted"));
      } finally {
        reader.close();
      }
      // Because the first reader was returned to the pool, this one should work.
      final KijiTableReader reader3 = pool.borrowObject();
      reader3.close();
    } finally {
      pool.close();
    }
  }
}
