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

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Interface for table reader factories.
 *
 * <p> Use <code>KijiTable.getReaderFactory()</code> to get a reader.
 */
@ApiAudience.Public
@ApiStability.Experimental
public interface KijiReaderFactory {

  /**
   * Opens a new reader for the KijiTable associated with this reader factory.
   *
   * <p> The caller of this method is responsible for closing the reader. </p>
   * <p>
   *   The reader returned by this method does not provide any isolation guarantee.
   *   In particular, you should assume that the underlying resources (connections, buffers, etc)
   *   are used concurrently for other purposes.
   * </p>
   *
   * @return a new KijiTableReader.
   * @throws IOException on I/O error.
   */
  KijiTableReader openTableReader() throws IOException;
}
