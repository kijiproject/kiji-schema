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
package org.kiji.schema.impl.async;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import junit.framework.Assert;
import org.apache.avro.util.Utf8;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.AsyncKijiResultScanner;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiFuture;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

public class TestAsyncHBaseAsyncKijiResultScanner extends KijiClientTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestAsyncHBaseAsyncKijiResultScanner.class);
  private static final String LAYOUT_PATH = "org/kiji/schema/layout/all-types-schema.json";
  private static final String TABLE_NAME = "all_types_table";

  private static final KijiScannerOptions OPTIONS = new KijiScannerOptions();
  private static final KijiColumnName PRIMITIVE_STRING =
      KijiColumnName.create("primitive", "string_column");
  private static final KijiDataRequest REQUEST = KijiDataRequest.builder().addColumns(
      ColumnsDef.create().withMaxVersions(10).add(PRIMITIVE_STRING)
  ).build();

  private AsyncHBaseKijiTable mTable;
  private AsyncHBaseAsyncKijiTableReader mReader;
  private Kiji mAsyncHBaseKiji;

  // Variables for the Asynchronous test
  private CountDownLatch latch;
  private final AtomicBoolean hasError = new AtomicBoolean(false);
  private final AtomicInteger rowCount = new AtomicInteger(0);
  private final Set<KijiResult<Utf8>> resultSet =
      Collections.synchronizedSet(new HashSet<KijiResult<Utf8>>());
  private final FutureCallback<KijiResult<Utf8>> resultFutureCallback =
      new FutureCallback<KijiResult<Utf8>>() {
        @Override
        public void onSuccess(@Nullable final KijiResult<Utf8> result) {
          if (null == result) {
            latch.countDown();
          } else {
            resultSet.add(result);
          }
        }

        @Override
        public void onFailure(final Throwable t) {
          if (!t.getMessage().equals(
              "You must wait for the previous KijiFuture to resolve before calling next().")) {
            hasError.getAndSet(true);
            Assert.fail("Failed: " + t.toString() + "\n"+ t.getStackTrace());
          } else {
            try {
              Thread.sleep(10);
            } catch (Exception e) {
              hasError.getAndSet(true);
              Assert.fail(e.toString());
            }
            fetchMoreResults();
          }
        }
      };
  private AsyncKijiResultScanner<Utf8> mScanner;

  @Before
  public void setupTestAsyncHBaseKijiResultScanner() throws Exception  {
    Kiji tempKiji = getKiji();
    new InstanceBuilder(tempKiji)
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
    mAsyncHBaseKiji = new AsyncHBaseKijiFactory().open(tempKiji.getURI());
    mTable = (AsyncHBaseKijiTable) mAsyncHBaseKiji.openTable(TABLE_NAME);
    mReader = (AsyncHBaseAsyncKijiTableReader) mTable.getReaderFactory().openAsyncTableReader();
  }

  @After
  public void cleanupTestHBaseKijiResultScanner() throws IOException {
    mReader.close();
    mTable.release();
    mAsyncHBaseKiji.release();
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
  public void testResultScannerSynchronously() throws IOException {
    for (int rowCacheSize : ImmutableList.of(0, 2, 3, 10)) {
      LOG.info("rowCacheSize = {}", rowCacheSize);
      final KijiScannerOptions options = new KijiScannerOptions();
      options.setRowCaching(rowCacheSize);
      options.setReopenScannerOnTimeout(false);
      final AsyncKijiResultScanner<Utf8> scanner = mReader.getKijiResultScanner(REQUEST, options);
      final Function<Utf8, String> toString = new ToString<Utf8>();
      try {
        int rowCount = 0;
        KijiResult<Utf8> result = null;
        try {
          result = scanner.next().get();
        } catch (Exception e) {
          Assert.fail(e.toString());
        }
        while (null != result) {
          final List<String> values;
          try {
            values = ImmutableList.copyOf(
                Iterables.transform(
                    KijiResult.Helpers.getValues(result),
                    toString));
            result.close();
          } catch (Exception e) {
            ZooKeeperUtils.wrapAndRethrow(e);
            throw new InternalKijiError(e);
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
          final KijiFuture<KijiResult<Utf8>> futureResult = scanner.next();
          try {
            result = futureResult.get();
          } catch (Exception e) {
            Assert.fail(e.toString());
          }
        }
        Assert.assertEquals(3, rowCount);
      } finally {
        scanner.close();
      }
    }
  }

  private void fetchMoreResults() {
    //Quickly call next() 5 times
    for (int i = 0; i < 5; i++ ) {
      Futures.addCallback(mScanner.next(), resultFutureCallback);
    }
  }

  @Test
  public void testResultScannerAsynchronously() throws Exception {
    final Function<Utf8, String> toString = new ToString<Utf8>();
    for (int rowCacheSize : ImmutableList.of(0, 1, 2, 3, 10)) {
      LOG.info("rowCacheSize = {}", rowCacheSize);
      latch = new CountDownLatch(1);
      rowCount.set(0);
      final KijiScannerOptions options = new KijiScannerOptions();
      options.setRowCaching(rowCacheSize);
      options.setReopenScannerOnTimeout(false);
      mScanner = mReader.getKijiResultScanner(REQUEST, options);
      fetchMoreResults();
      Assert.assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
      mScanner.close();
      int rowCount = 0;
      for (KijiResult<Utf8> result : resultSet) {
        final List<String> values;
        try {
          values = ImmutableList.copyOf(
              Iterables.transform(
                  KijiResult.Helpers.getValues(result),
                  toString));
          result.close();
        } catch (Exception e) {
          ZooKeeperUtils.wrapAndRethrow(e);
          throw new InternalKijiError(e);
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
      Assert.assertFalse(hasError.get());
      resultSet.clear();
    }
  }
}
