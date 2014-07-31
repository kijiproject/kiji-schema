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
import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Interface for performing asynchronous puts on a Kiji table.
 *
 * <p>
 *   AsyncKijiPutter provides methods for putting values into cells given an entity id,
 *   column family, column qualifier, and optional timestamp, along with the value to put.
 *   If a timestamp is not specified, the current system time should be used for the put.
 * </p>
 * <pre>
 *   final KijiPutter putter = myKijiTable.openTableWriter();
 *   putter.put(entityId, columnFamily, columnQualifier, timestamp, value);
 *   putter.put(entityId, columnFamily, columnQualifier, value);
 * </pre>
 *
 * <p> This interface is not used alone but is bundled within
 * {@link org.kiji.schema.KijiTableWriter}. </p>
 * <p>
 *   Unless otherwise specified, putters are not thread-safe and must be synchronized externally.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface AsyncKijiPutter extends Closeable {
  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param columnName The KijiColumName to put data into.
   * @param value The data to write.
   * @param <T> The type of the value being written.
   * @return A {@code KijiFuture} object that indicates the completion of the request. The Object
   * has no special meaning and can be null (think of it as KijiFuture<Void>). But you probably want
   * to attach at least a {@link com.google.common.util.concurrent.FutureCallback}  in order to
   * handle failures.
   * @throws java.io.IOException If there is an IO error.
   */
  <T> KijiFuture<Object> put(EntityId entityId, KijiColumnName columnName, T value)
      throws IOException;

  /**
   * Puts data into a kiji table.
   *
   * @param entityId The entity (row) to put data into.
   * @param columnName The KijiColumName to put data into.
   * @param timestamp Timestamp, in millisecond since the Epoch.
   * @param value The data to write.
   * @param <T> The type of the value being written.
   * @return A {@code KijiFuture} object that indicates the completion of the request. The Object
   * has no special meaning and can be null (think of it as KijiFuture<Void>). But you probably want
   * to attach at least a {@link com.google.common.util.concurrent.FutureCallback}  in order to
   * handle failures.
   * @throws java.io.IOException If there is an IO error.
   */
  <T> KijiFuture<Object> put(EntityId entityId, KijiColumnName columnName, long timestamp, T value)
      throws IOException;
}
