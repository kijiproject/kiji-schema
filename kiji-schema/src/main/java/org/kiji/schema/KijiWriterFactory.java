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
 * Interface for table writer factories.
 *
 * <p> Use <code>KijiTable.getWriterFactory()</code> to get a writer.
 */
@ApiAudience.Public
@ApiStability.Evolving
public interface KijiWriterFactory {

  /**
   * Opens a new KijiTableWriter for the KijiTable associated with this writer factory.
   * The caller of this method is responsible for closing the writer.
   *
   * @return A new KijiTableWriter.
   * @throws IOException in case of an error.
   */
  KijiTableWriter openTableWriter() throws IOException;

  /**
   * Opens a new AtomicKijiPutter for the KijiTable associated with this writer factory.
   * The caller of this method is responsible for closing the writer.
   *
   * @return A new AtomicKijiPutter.
   * @throws IOException in case of an error.
   */
  AtomicKijiPutter openAtomicPutter() throws IOException;

  /**
   * Opens a new KijiBufferedWriter for the KijiTable associated with this writer factory.
   * The caller of this method is responsible for closing the writer.
   *
   * @return A new KijiBufferedWriter.
   * @throws IOException in case of an error.
   */
  KijiBufferedWriter openBufferedWriter() throws IOException;
}
