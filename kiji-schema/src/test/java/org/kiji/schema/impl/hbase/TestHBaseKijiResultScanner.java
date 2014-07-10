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
package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.avro.util.Utf8;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestHBaseKijiResultScanner extends KijiClientTest {

  private static final String LAYOUT_PATH = "org/kiji/schema/layout/all-types-schema.json";
  private static final String TABLE_NAME = "all_types_table";

  private static final KijiScannerOptions OPTIONS = new KijiScannerOptions();
  private static final KijiColumnName PRIMITIVE_STRING =
      KijiColumnName.create("primitive", "string_column");
  private static final KijiDataRequest REQUEST = KijiDataRequest.builder().addColumns(
      ColumnsDef.create().withMaxVersions(10).add(PRIMITIVE_STRING)
  ).build();

  private HBaseKijiTable mTable;
  private HBaseKijiTableReader mReader;

  @Before
  public void setupTestHBaseKijiResultScanner() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(LAYOUT_PATH))
            .withRow(1)
                .withFamily("primitive")
                    .withQualifier("string_column")
                        .withValue(10, "ten")
                        .withValue(5, "five")
                        .withValue(4, "four")
                        .withValue(3, "three")
                        .withValue(2, "two")
                        .withValue(1, "one")
            .withRow(2)
                .withFamily("primitive")
                    .withQualifier("string_column")
                        .withValue(20, "twenty")
                        .withValue(15, "fifteen")
                        .withValue(14, "fourteen")
                        .withValue(13, "thirteen")
                        .withValue(12, "twelve")
                        .withValue(11, "eleven")
            .withRow(3)
                .withFamily("primitive")
                    .withQualifier("string_column")
                        .withValue(30, "thirty")
                        .withValue(25, "twenty five")
                        .withValue(24, "twenty four")
                        .withValue(23, "twenty three")
                        .withValue(22, "twenty two")
                        .withValue(21, "twenty one")
        .build();
    mTable = (HBaseKijiTable) getKiji().openTable(TABLE_NAME);
    mReader = (HBaseKijiTableReader) mTable.openTableReader();
  }

  @After
  public void cleanupTestHBaseKijiResultScanner() throws IOException {
    mReader.close();
    mTable.release();
  }

  /**
   * Simple function to convert an object to a String.
   */
  private static final class ToString<T> implements Function<T, String> {
    @Override
    public String apply(final T input) {
      return input.toString();
    }
  }

  @Test
  public void test() throws IOException {
    final HBaseKijiResultScanner<Utf8> scanner = mReader.getKijiResultScanner(REQUEST, OPTIONS);
    final Function<Utf8, String> toString = new ToString<Utf8>();
    try {
      int rowCount = 0;
      while (scanner.hasNext()) {
        final KijiResult<Utf8> result = scanner.next();
        final List<String> values;
        try {
          values = ImmutableList.copyOf(
              Iterables.transform(
                  KijiResult.Helpers.getValues(result),
                  toString));
        } finally {
          result.close();
        }

        rowCount++;
        final Long entity = result.getEntityId().getComponentByIndex(0);
        // Hashing may scramble the order of the rows, so we have to check which row we're on to
        // test them individually.
        final List<String> expected;
        if (entity == 1) {
          expected = Lists.newArrayList("ten", "five", "four", "three", "two", "one");
        } else if (entity == 2) {
          expected =
              Lists.newArrayList("twenty", "fifteen", "fourteen", "thirteen", "twelve", "eleven");
        } else if (entity == 3) {
          expected = Lists.newArrayList(
              "thirty", "twenty five", "twenty four", "twenty three", "twenty two", "twenty one");
        } else {
          expected = null;
          Assert.fail("should only find entities 1, 2, 3");
        }
        Assert.assertEquals(expected, values);
      }
      Assert.assertEquals(3, rowCount);
    } finally {
      scanner.close();
    }
  }
}
