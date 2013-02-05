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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Interface for Kiji Tables.
 *
 * Instantiated from {@link KijiTableFactory#openTable(String)}
 */
@ApiAudience.Public
@Inheritance.Sealed
public interface KijiTable extends Closeable {
  /** @return the Kiji instance this table belongs to. */
  Kiji getKiji();

  /** @return the name of this table. */
  String getName();

  /** @return the URI for this table, trimmed at the table path component. */
  KijiURI getURI();

  /** @return the layout of this table. */
  KijiTableLayout getLayout();

  /**
   * Creates an entity ID from a list of key components.
   *
   * @param kijiRowKey This can be one of the following depending on row key encoding:
   *                   <ul>
   *                      <li>
   *                      Raw, Hash, Hash-Prefix EntityId: A single String or byte array
   *                      component.
   *                      </li>
   *                      <li>
   *                      Formatted EntityId: The primitive row key components (string, int,
   *                      long) either passed in their expected order in the key or as an ordered
   *                      list of components.
   *                      </li>
   *                   </ul>
   * @return a new EntityId with the specified Kiji row key.
   */
  EntityId getEntityId(Object... kijiRowKey);

  /**
   * Opens an appropriate implementation of KijiTableReader for this table.  The caller is
   * responsible for closing this reader.
   *
   * @throws IOException If there was an error opening the reader.
   * @return A KijiTableReader for this table.
   */
  KijiTableReader openTableReader() throws IOException;

  /**
   * Opens an appropriate implementation of KijiTableWriter for this table.  The caller is
   * responsible for closing this writer.
   *
   * @throws IOException If there was an error opening the writer.
   * @return A KijiTableWriter for this table.
   */
  KijiTableWriter openTableWriter() throws IOException;

  /**
   * Return the regions in this table as an ordered list.
   *
   * @return An ordered list of the table regions.
   * @throws IOException on I/O error.
   */
  List<KijiRegion> getRegions() throws IOException;
}
