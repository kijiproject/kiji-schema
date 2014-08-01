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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.stumbleupon.async.Deferred;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiFuture;
import org.kiji.schema.KijiClientTest;

/** Tests for AsyncHBaseKijiFuture. */
public class TestAsyncHBaseKijiFuture extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncHBaseKijiFuture.class);

  private void testString(String str) {
    LOG.info("Testing String");
    Assert.assertEquals("Hello World", str);
  }

  @Test
  public void testKijiFuture() throws Exception {
    final CountDownLatch latch = new CountDownLatch(2);
    final Deferred<String> deferredString = Deferred.fromResult("Hello World");
    final KijiFuture<String> future = AsyncHBaseKijiFuture.create(deferredString);
    Futures.addCallback(future, new FutureCallback<String>() {
          @Override
          public void onSuccess(@Nullable final String s) {
            LOG.info("Calling testString()");
            testString(s);
            latch.countDown();
          }

          @Override
          public void onFailure(final Throwable throwable) {
            Assert.fail("Should not fail");
          }
        });

    final KijiFuture<Object> future2 = AsyncHBaseKijiFuture.create();
    Futures.addCallback(future2, new FutureCallback<Object>() {
          @Override
          public void onSuccess(@Nullable final Object o) {
            LOG.info("future2: onSuccess");
            Assert.assertNull(o);
            latch.countDown();
          }

          @Override
          public void onFailure(final Throwable throwable) {
            Assert.fail("Should not fail");
          }
        });

    latch.await(2, TimeUnit.SECONDS);
  }

  @Test
  public void testDeferredError() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final String errorMessage = "Deferred Exception";
    final Deferred<Object> error = Deferred.fromError(new Exception(errorMessage));
    AsyncHBaseKijiFuture<Object> future = AsyncHBaseKijiFuture.create(error);
    //Will be called immediately since the future already has a value.
    Futures.addCallback(future, new FutureCallback<Object>() {
          @Override
          public void onSuccess(@Nullable final Object o) {
            LOG.info("future: onSuccess");
            Assert.fail("Should not succeed");
          }

          @Override
          public void onFailure(final Throwable throwable) {
            LOG.info("future: onFailure");
            Assert.assertEquals(errorMessage, throwable.getMessage());
            latch.countDown();
          }
        });
    latch.await(1, TimeUnit.SECONDS);
  }

  @Test
  public void testTransforms() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final String intAsString = "100";
    final Deferred<String> deferredStr = Deferred.fromResult(intAsString);
    AsyncHBaseKijiFuture<String> future = AsyncHBaseKijiFuture.create(deferredStr);
    Futures.addCallback(future, new FutureCallback<String>() {
          @Override
          public void onSuccess(@Nullable final String s) {
            LOG.info("future: onSuccess");
            latch.countDown();
          }

          @Override
          public void onFailure(final Throwable throwable) {
            LOG.info("future: onFailure");
            Assert.fail("Should not fail.");
          }
        });
    ListenableFuture<Integer> intFuture1 = Futures.transform(future, new Function<String, Integer>() {
          @Nullable
          @Override
          public Integer apply(@Nullable final String str) {
            return new Integer(str);
          }
        });
    ListenableFuture<Integer> intFuture2 = Futures.transform(intFuture1, new Function<Integer, Integer>() {
          @Nullable
          @Override
          public Integer apply(@Nullable final Integer i) {
            if (null == i ) {
              Assert.fail();
            }
            return (i - 99);
          }
        });
    latch.await(2, TimeUnit.SECONDS);
    Assert.assertEquals(new Integer(100), intFuture1.get());
    Assert.assertEquals(new Integer(1), intFuture2.get());
  }
}
