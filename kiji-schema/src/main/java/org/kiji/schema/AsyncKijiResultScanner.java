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
package org.kiji.schema;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.Future;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * An asynchronous scanner over rows in a KijiTable. Rows are returned as a
 * {@link org.kiji.schema.KijiFuture}<{@link org.kiji.schema.KijiResult}>s.
 * {@code AsyncKijiResultScanner} must be closed when it will no longer be used.
 *
 * <p><b>NOTE:</b></p> AsyncKijiResultScanner does not support
 *
 * @param <T> type of {@code KijiCell} value returned by scanned {@code KijiFuture<KijiResult>}s.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
public interface AsyncKijiResultScanner<T> extends Closeable {

  /**
   * Get a KijiFuture that will contain the next KijiResult once it has returned.
   *
   * <p>Note that the scanning is complete when the returned KijiFuture contains null. Every
   * subsequent call to next() will result in a KijiFuture with a value of null.</p>
   *
   * @return A KijiFuture that will contain the next KijiResult once it is available.
   */
  public KijiFuture<KijiResult<T>> next();
}
