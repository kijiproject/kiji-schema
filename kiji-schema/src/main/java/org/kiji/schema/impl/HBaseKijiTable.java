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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiRegion;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.ResourceUtils;

/**
 * <p>A KijiTable that exposes the underlying HBase implementation.</p>
 *
 * <p>Within the internal Kiji code, we use this class so that we have
 * access to the HTable interface.  Methods that Kiji clients should
 * have access to should be added to org.kiji.schema.KijiTable.</p>
 */
@ApiAudience.Private
public class HBaseKijiTable implements KijiTable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiTable.class);
  // private static final Logger CLEANUP_LOG =
  //     LoggerFactory.getLogger(HBaseKijiTable.class.getName() + ".Cleanup");
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + HBaseKijiTable.class.getName());

  /** The kiji instance this table belongs to. */
  private final HBaseKiji mKiji;

  /** The name of this table (the Kiji name, not the HBase name). */
  private final String mName;

  /** URI of this table. */
  private final KijiURI mTableURI;

  /** Whether the table is open. */
  private boolean mIsOpen;

  /** String representation of the call stack at the time this object is constructed. */
  private String mConstructorStack;

  /** The underlying HTable that stores this Kiji table's data. */
  private final HTableInterface mHTable;

  /** The layout of the Kiji table. */
  private final KijiTableLayout mTableLayout;

  /** The factory for EntityIds. */
  private final EntityIdFactory mEntityIdFactory;

  /** Retain counter. When decreased to 0, the HBase KijiTable may be closed and disposed of. */
  private AtomicInteger mRetainCount = new AtomicInteger(1);

  /**
   * Construct an opened Kiji table stored in HBase.
   *
   * @param kiji The Kiji instance.
   * @param name The name of the Kiji user-space table to open.
   * @param conf The Hadoop configuration object.
   *
   * @throws IOException On an HBase error.
   */
  HBaseKijiTable(HBaseKiji kiji, String name, Configuration conf) throws IOException {
    this(kiji, name, conf, DefaultHTableInterfaceFactory.get());
  }

  /**
   * Construct an opened Kiji table stored in HBase.
   *
   * @param kiji The Kiji instance.
   * @param name The name of the Kiji user-space table to open.
   * @param conf The Hadoop configuration object.
   * @param htableFactory A factory that creates HTable objects.
   *
   * @throws IOException On an HBase error.
   */
  HBaseKijiTable(HBaseKiji kiji, String name, Configuration conf,
      HTableInterfaceFactory htableFactory) throws IOException {
    mKiji = kiji;
    mKiji.retain();
    mName = name;
    mTableURI = KijiURI.newBuilder(mKiji.getURI()).withTableName(mName).build();
    mTableLayout = mKiji.getMetaTable().getTableLayout(name);
    try {
      mHTable = htableFactory.create(conf,
          KijiManagedHBaseTableName.getKijiTableName(kiji.getURI().getInstance(), name).toString());
    } catch (TableNotFoundException e) {
      close();
      throw new KijiTableNotFoundException(name);
    }

    if (mTableLayout.getDesc().getKeysFormat() instanceof RowKeyFormat) {
      mEntityIdFactory = EntityIdFactory.getFactory((RowKeyFormat) mTableLayout.getDesc()
          .getKeysFormat());
    } else if (mTableLayout.getDesc().getKeysFormat() instanceof RowKeyFormat2) {
      mEntityIdFactory = EntityIdFactory.getFactory((RowKeyFormat2) mTableLayout.getDesc()
          .getKeysFormat());
    } else {
      throw new RuntimeException("Invalid Row Key format found in Kiji Table");
    }

    mIsOpen = true;
    if (CLEANUP_LOG.isDebugEnabled()) {
      mConstructorStack = Debug.getStackTrace();
    }
  }

  /** {@inheritDoc} **/
  @Override
  public EntityId getEntityId(Object... kijiRowKey) {
    return mEntityIdFactory.getEntityId(kijiRowKey);
  }

  /** {@inheritDoc} */
  @Override
  public Kiji getKiji() {
    return mKiji;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return mName;
  }

  /** {@inheritDoc} */
  @Override
  public KijiURI getURI() {
    return mTableURI;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout getLayout() {
    return mTableLayout;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableReader openTableReader() {
    return new HBaseKijiTableReader(this);
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableWriter openTableWriter() {
    return new HBaseKijiTableWriter(this);
  }

  /**
   * Return the regions in this table as a list.
   *
   * <p>This method was copied from HFileOutputFormat of 0.90.1-cdh3u0 and modified to
   * return KijiRegion instead of ImmutableBytesWritable.</p>
   *
   * @return An ordered list of the table regions.
   * @throws IOException on I/O error.
   */
  @Override
  public List<KijiRegion> getRegions() throws IOException {
    final HBaseAdmin hbaseAdmin = ((HBaseKiji) getKiji()).getHBaseAdmin();
    final HTableInterface hbaseTable = getHTable();

    final List<HRegionInfo> regions = hbaseAdmin.getTableRegions(hbaseTable.getTableName());
    final List<KijiRegion> result = Lists.newArrayList();

    // If we can get the concrete HTable, we can get location information.
    if (hbaseTable instanceof HTable) {
      LOG.debug("Casting HTableInterface to an HTable.");
      final HTable concreteHBaseTable = (HTable) hbaseTable;
      try {
        for (HRegionInfo region: regions) {
          List<HRegionLocation> hLocations =
              concreteHBaseTable.getRegionsInRange(region.getStartKey(), region.getEndKey());
          result.add(new HBaseKijiRegion(region, hLocations));
        }
      } finally {
        ResourceUtils.closeOrLog(concreteHBaseTable);
      }
    } else {
      LOG.warn("Unable to cast HTableInterface {} to an HTable.  "
          + "Creating Kiji regions without location info.", getURI());
      for (HRegionInfo region: regions) {
        result.add(new HBaseKijiRegion(region));
      }
    }

    return result;
  }

  /** @return The underlying HTable instance. */
  public HTableInterface getHTable() {
    return mHTable;
  }

  /**
   * Releases all the resources used by this Kiji instance.
   *
   * @throws IOException on I/O error.
   */
  private void close() throws IOException {
    synchronized (this) {
      if (!mIsOpen) {
        LOG.warn("close() called on an KijiTable that was already closed.");
        return;
      }
      mIsOpen = false;
    }

    LOG.debug(String.format("Closing KijiTable '%s'.", mTableURI));
    if (null != mHTable) {
      mHTable.close();
    }
    mKiji.release();
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable retain() {
    Preconditions.checkState(mRetainCount.incrementAndGet() >= 0,
        "Cannot retain a closed KijiTable.");
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void release() throws IOException {
    final int counter = mRetainCount.decrementAndGet();
    Preconditions.checkState(counter >= 0, "Cannot release resource: retain counter is 0.");
    if (counter == 0) {
      close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (null == obj) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!getClass().equals(obj.getClass())) {
      return false;
    }
    final KijiTable other = (KijiTable) obj;

    // Equal if the two tables have the same URI:
    return mTableURI.equals(other.getURI());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mTableURI.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      CLEANUP_LOG.warn(
          "Closing HBaseKijiTable {} in finalize(). You should close it explicitly",
          mTableURI);
      CLEANUP_LOG.warn(
          "HBaseKijiTable {} still has {} retained references.",
          mTableURI,
          mRetainCount.get());
      if (CLEANUP_LOG.isDebugEnabled()) {
        CLEANUP_LOG.debug(
            "Call stack when this HBaseKijiTable was constructed:\n{}",
            mConstructorStack);
      }
      close();
    }
    super.finalize();
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
}
