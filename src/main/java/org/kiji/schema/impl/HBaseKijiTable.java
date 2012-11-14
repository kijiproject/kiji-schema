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

package org.kiji.schema.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;

import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiManagedHBaseTableName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayout;


/**
 * <p>A KijiTable that exposes the underlying HBase implementation.</p>
 *
 * <p>Within the internal Kiji code, we use this class so that we have
 * access to the HTable interface.  Methods that Kiji clients should
 * have access to should be added to org.kiji.schema.KijiTable.</p>
 */
public class HBaseKijiTable extends AbstractKijiTable {
  /** The underlying HTable that stores this Kiji table's data. */
  private final HTable mHTable;

  /** The layout of the Kiji table. */
  private final KijiTableLayout mTableLayout;

  /** The factory for EntityIds. */
  private final EntityIdFactory mEntityIdFactory;

  /**
   * Construct an opened Kiji table stored in HBase.
   *
   * @param kiji The Kiji instance.
   * @param name The name of the Kiji user-space table to open.
   *
   * @throws IOException On an HBase error.
   */
  public HBaseKijiTable(Kiji kiji, String name) throws IOException {
    this(kiji, name, new HTableFactory());
  }

  /**
   * Construct an opened Kiji table stored in HBase.
   *
   * @param kiji The Kiji instance.
   * @param name The name of the Kiji user-space table to open.
   * @param htableFactory A factory that creates HTable objects.
   *
   * @throws IOException On an HBase error.
   */
  public HBaseKijiTable(Kiji kiji, String name, HTableFactory htableFactory)
      throws IOException {
    super(kiji, name);
    try {
      mHTable = htableFactory.create(kiji.getConf(),
          KijiManagedHBaseTableName.getKijiTableName(kiji.getName(), name).toString());
    } catch (TableNotFoundException e) {
      super.close();
      throw new KijiTableNotFoundException(name);
    }
    mTableLayout = kiji.getMetaTable().getTableLayout(name);
    mEntityIdFactory = EntityIdFactory.create(mTableLayout.getDesc().getKeysFormat());
  }

  /**
   * We know that all KijiTables are really HBaseKijiTables
   * instances.  This is a convenience method for downcasting, which
   * is common within the internals of Kiji code.
   *
   * @param kijiTable The Kiji table to downcast to an HBaseKijiTable.
   * @return The given Kiji table as an HBaseKijiTable.
   */
  public static HBaseKijiTable downcast(KijiTable kijiTable) {
    if (!(kijiTable instanceof HBaseKijiTable)) {
      // This should really never happen.  Something is seriously
      // wrong with Kiji code if we get here.
      throw new InternalKijiError(
          "Found a KijiTable object that was not an instance of HBaseKijiTable.");
    }
    return (HBaseKijiTable) kijiTable;
  }

  /** @return The underlying HTable instance. */
  public HTable getHTable() {
    return mHTable;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout getLayout() {
    return mTableLayout;
  }

  /** {@inheritDoc} */
  @Override
  public EntityIdFactory getEntityIdFactory() {
    return mEntityIdFactory;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableReader openTableReader() {
    return new HBaseKijiTableReader(this);
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableWriter openTableWriter() throws IOException {
    return new HBaseKijiTableWriter(this);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mHTable.close();
    super.close();
  }
}
