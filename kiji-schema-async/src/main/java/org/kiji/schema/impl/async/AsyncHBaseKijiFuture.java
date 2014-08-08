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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiFuture;

/**
 * A KijiFuture that converts a {@code Deferred} object into a {@code ListenableFuture}.
 */
@ApiAudience.Private
public final class AsyncHBaseKijiFuture<T> implements KijiFuture<T> {

  final ListenableFuture<T> mListenableFuture;

  public static <T> AsyncHBaseKijiFuture<T> create() {
    return createFromFuture(null);
  }

  public static <T> AsyncHBaseKijiFuture<T> createFromFuture(ListenableFuture<T> future) {
    return new AsyncHBaseKijiFuture<T>(future);
  }

  public static <T> AsyncHBaseKijiFuture<T> create(Deferred<T> deferred) {
    return new AsyncHBaseKijiFuture<T>(deferred);
  }

  AsyncHBaseKijiFuture(ListenableFuture<T> future) {
    mListenableFuture = future;
  }

  AsyncHBaseKijiFuture(Deferred<T> deferred) {
    final SettableFuture<T> sf = SettableFuture.create();
    if (null != deferred) {
      deferred.addCallbacks(
          new Callback<T, T>() {
            @Override
            public T call(final T o) throws Exception {
              sf.set(o);
              return o;
            }
          }, new Callback<Exception, T>() {
            @Override
            public Exception call(final T o) throws Exception {
              sf.setException((Exception) o);
              return (Exception) o;
            }
          });
    } else {
      sf.set(null);
    }
    mListenableFuture = sf;
  }

  /** {inheritDoc} */
  @Override
  @ParametersAreNonnullByDefault
  public void addListener(final Runnable runnable, final Executor executor) {
    mListenableFuture.addListener(runnable, executor);
  }

  /** {inheritDoc} */
  @Override
  public boolean cancel(final boolean b) {
    return mListenableFuture.cancel(b);
  }

  /** {inheritDoc} */
  @Override
  public boolean isCancelled() {
    return mListenableFuture.isCancelled();
  }

  /** {inheritDoc} */
  @Override
  public boolean isDone() {
    return mListenableFuture.isDone();
  }

  /** {inheritDoc} */
  @Override
  public T get() throws InterruptedException, ExecutionException {
    return mListenableFuture.get();
  }

  /** {inheritDoc} */
  @Override
  public T get(final long l, final TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return mListenableFuture.get(l, timeUnit);
  }
}
