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

  /** @return the entity ID factory for this table. */
  EntityIdFactory getEntityIdFactory();

  /**
   * Creates an entity ID for the specified UTF8 encoded Kiji row key.
   *
   * @param kijiRowKey UTF8 encoded Kiji row key.
   * @return a new entity ID for the specified Kiji row key.
   */
  EntityId getEntityId(String kijiRowKey);

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
}
