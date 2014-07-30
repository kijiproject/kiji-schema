/**
 * (c) Copyright 2012 WibiData, Inc.
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
import java.util.List;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ReferenceCountable;

/**
 * The KijiTable interface provides operations on KijiTables. To perform reads to and
 * writes from a Kiji table use a {@link KijiTableReader} or {@link KijiTableWriter}.
 * Instances of these classes can be obtained by using the {@link #openTableReader()}
 * and {@link #openTableWriter()} methods.  {@link EntityId}s, which identify particular
 * rows within a Kiji table are also generated from its components.
 *
 * <h2>KijiTable instance lifecycle:</h2>
 * <p>
 *   To open a connection to a KijiTable, use {@link Kiji#openTable(String)}. A KijiTable
 *   contains an open connection to an HBase cluster. Because of this, KijiTable objects
 *   must be released using {@link #release()} when finished using it:
 * </p>
 * <pre>
 *   <code>
 *     final KijiTable table = myKiji.openTable("tableName");
 *     // Do some magic
 *     table.release();
 *   </code>
 * </pre>
 *
 * <h2>Reading & Writing from a KijiTable:</h2>
 * <p>
 *   The KijiTable interface does not directly provide methods to perform I/O on a Kiji
 *   table. Read and write operations can be performed using either a {@link KijiTableReader}
 *   or a {@link KijiTableWriter}:
 * </p>
 * <pre>
 *   <code>
 *     final KijiTable table = myKiji.openTable("tableName");
 *
 *     final EntityId myId = table.getEntityId("myRowKey");
 *
 *     final KijiTableReader reader = table.openTableReader();
 *     final KijiTableWriter writer = table.openTableWriter();
 *
 *     // Read some data from a Kiji table using an existing EntityId and KijiDataRequest.
 *     final KijiRowData row = reader.get(myId, myDataRequest);
 *
 *     // Do things with the row...
 *
 *     // Write some data to a new column in the same row.
 *     writer.put(myId, "info", "newcolumn", "newvalue");
 *
 *     // Close open connections.
 *     reader.close();
 *     writer.close();
 *     table.release();
 *   </code>
 * </pre>
 *
 * @see KijiTableReader for more information about reading data from a Kiji table.
 * @see KijiTableWriter for more information about writing data to a Kiji table.
 * @see EntityId for more information about identifying rows with entity ids.
 * @see Kiji for more information about opening a KijiTable instance.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public interface KijiTable extends ReferenceCountable<KijiTable> {
  /** @return the Kiji instance this table belongs to. */
  Kiji getKiji();

  /** @return the name of this table. */
  String getName();

  /** @return the URI for this table, trimmed at the table path component. */
  KijiURI getURI();

  /** @return the layout of this table. */
  KijiTableLayout getLayout();

  /**
   * Creates an entity id from a list of components.
   *
   * @param kijiRowKey This can be one of the following depending on row key encoding:
   *     <ul>
   *       <li>
   *         Raw, Hash, Hash-Prefix EntityId: A single String or byte array
   *         component.
   *       </li>
   *       <li>
   *         Formatted EntityId: The primitive row key components (string, int,
   *         long) either passed in their expected order in the key or as an ordered
   *         list of components.
   *       </li>
   *     </ul>
   * @return a new EntityId with the specified Kiji row key.
   */
  EntityId getEntityId(Object... kijiRowKey);

  /**
   * Opens a KijiTableReader for this table.
   *
   * <p>
   *   This method is equivalent to <code>getReaderFactory().readerBuilder().build()</code>. It sets
   *   all options to their default values.
   * </p>
   *
   * <p> The caller of this method is responsible for closing the returned reader. </p>
   * <p> The reader returned by this method does not provide any isolation guarantee.
   *     In particular, you should assume that the underlying resources (connections, buffers, etc)
   *     are used concurrently for other purposes. </p>
   *
   * @return A KijiTableReader for this table.
   * @throws KijiIOException Future implementations may throw unchecked KijiIOException.
   */
  KijiTableReader openTableReader();

  /**
   * Gets a KijiReaderFactory for this table.
   *
   * <p> The returned reader factory is valid as long as the caller retains the table. </p>
   *
   * @throws IOException in case of an error.
   * @return A KijiReaderFactory.
   */
  KijiReaderFactory getReaderFactory() throws IOException;

  /**
   * Opens a KijiTableWriter for this table.
   *
   * <p> The caller of this method is responsible for closing the returned writer.
   * <p> The writer returned by this method does not provide any isolation guarantee.
   *     In particular, you should assume that the underlying resources (connections, buffers, etc)
   *     are used concurrently for other purposes.
   * <p> If you have specific resource requirements, such as buffering, timeouts, dedicated
   *     connection, etc, use {@link #getWriterFactory()}.
   *
   * @return A KijiTableWriter for this table.
   * @throws KijiIOException Future implementations may throw unchecked KijiIOException.
   */
  KijiTableWriter openTableWriter();

  /**
   * Gets a KijiWriterFactory for this table.
   *
   * <p> The returned writer factory is valid as long as the caller retains the table. </p>
   *
   * @throws IOException in case of an error.
   * @return A KijiWriterFactory.
   */
  KijiWriterFactory getWriterFactory() throws IOException;

  /**
   * Return the regions in this table as an ordered list.
   *
   * @return An ordered list of the table regions.
   * @throws IOException If there is an error retrieving the regions of this table.
   */
  List<KijiRegion> getRegions() throws IOException;

  /**
   * Get a table and column annotator for this table.
   *
   * @return a table and column annotator for this table.
   * @throws IOException in case of an error getting the meta table.
   */
  KijiTableAnnotator openTableAnnotator() throws IOException;
}
