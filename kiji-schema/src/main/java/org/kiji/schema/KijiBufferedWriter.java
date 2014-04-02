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
import org.kiji.annotations.Inheritance;

/**
 * Interface for performing batch operations on a Kiji Table.  Buffered operations are stored in
 * local memory and flushed on explicit calls to {@link #flush()} or {@link #close()}.  The buffer
 * of an open writer cannot be relied upon to flush before JVM shutdown.
 * Accessible via {@link KijiTable#getWriterFactory()} then
 * {@link KijiWriterFactory#openBufferedWriter()}.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface KijiBufferedWriter extends KijiPutter, KijiDeleter {
  /**
   * Set the size of the local write buffer (in bytes).
   *
   * @param bufferSize size (in bytes) to buffer before automatic flush.
   * @throws IOException in case of an error.
   */
  void setBufferSize(long bufferSize) throws IOException;

  /**
   * Commit any buffered writes.
   *
   * @throws IOException in case of an error.
   */
  void flush() throws IOException;
}
