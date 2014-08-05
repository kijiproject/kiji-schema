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
//
//package org.kiji.schema.impl.hbase;
//
//import java.io.Closeable;
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//
//import com.google.common.base.Objects;
//import org.apache.commons.pool.BasePoolableObjectFactory;
//import org.apache.commons.pool.impl.GenericObjectPool;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.client.Append;
//import org.apache.hadoop.hbase.client.Delete;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.HTableInterface;
//import org.apache.hadoop.hbase.client.Increment;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Row;
//import org.apache.hadoop.hbase.client.RowLock;
//import org.apache.hadoop.hbase.client.RowMutations;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.client.coprocessor.Batch;
//import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import org.kiji.annotations.ApiAudience;
//import org.kiji.schema.KijiIOException;
//import org.kiji.schema.hbase.KijiManagedHBaseTableName;
//import org.kiji.schema.impl.HTableInterfaceFactory;
//import org.kiji.schema.util.DebugResourceTracker;
//
///**
//* This private class is designed to serve as a replacement for HTablePool, to get around the
//* following HBase issues in the face of master failures:
//*
//* <ul>
//*   <li>https://issues.apache.org/jira/browse/HBASE-6580</li>
//*   <li>https://issues.apache.org/jira/browse/HBASE-6956</li>
//*   <li>https://issues.apache.org/jira/browse/HBASE-4805</li>
//* </ul>
//*
//* We accomplish this by creating a simple pool and a Pooled implementation of HTableInterface,
//* similar to how HTablePool works. Unlike HTablePool, however, we verify the underlying connection
//* before returning an instance from this pool. This implementation is also tied to a particular
//* table name.
//*
//* You may construct a KijiHTablePool using the constructor
//* {@link KijiHTablePool(String, org.kiji.schema.impl.HTableInterfaceFactory). Note that the
//* provided HTableInterfaceFactory <i>must</i> return HTables rather than another implementation of
//* HTableInterface.  We rely on this to test connectivity.
//*
//* Once created, use {@link #getTable()} to generate HTableInterface instances. They will be
//* returned to the pool automatically when closed.
//*
//* When you are finished with the pool, it should be closed to destroy any outstanding connections.
//*/
//@ApiAudience.Private
//public final class KijiHTablePool implements Closeable {
//  // TODO(SCHEMA-622): This functionality is rather large to fit into a bridge, but it should be
//  // migrated there once we support HBase 0.94.11 and later, which have a new way to make
//  // HTables.
//  private static final Logger LOG = LoggerFactory.getLogger(KijiHTablePool.class);
//
//  /** The name of the kiji table we're generating tables for. */
//  private final String mTableName;
//
//  /** The table's name in HBase. */
//  private final String mHBaseTableName;
//
//  /** The factory we'll use to generate our underlying HTableInterfaces. */
//  private final HTableInterfaceFactory mHTableFactory;
//
//  /** Our object pool. */
//  private final GenericObjectPool<PooledHTable> mPool;
//
//  /** The Kiji instance backing the table. */
//  private final HBaseKiji mKiji;
//
//  /**
//   * Primary constructor.
//   *
//   * @param name The name of the table that will be used with this pool.
//   * @param kiji The HBaseKiji instance backing these tables.
//   * @param tableFactory A factory to create the underlying HTableInterfaces.  This factory must
//   *     create HTables.
//   */
//  public KijiHTablePool(String name, HBaseKiji kiji, HTableInterfaceFactory tableFactory) {
//    mTableName = name;
//    mHTableFactory = tableFactory;
//    mKiji = kiji;
//
//    mHBaseTableName =
//        KijiManagedHBaseTableName.getKijiTableName(
//            mKiji.getURI().getInstance(),
//            mTableName)
//        .toString();
//
//    final GenericObjectPool.Config config = new GenericObjectPool.Config();
//
//    config.maxActive = -1;
//    config.maxIdle = -1;
//    // This state should not occur, so throw an exception.
//    config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_FAIL;
//    config.testOnBorrow = true;
//
//    mPool = new GenericObjectPool<PooledHTable>(new PooledHTableFactory(), config);
//    DebugResourceTracker.get().registerResource(this);
//  }
//
//  /**
//   * Returns an HTableInterface from the pool.
//   *
//   * @return An HTableInterface which should have a function Connection underneath it.
//   */
//  public HTableInterface getTable() {
//    try {
//      return mPool.borrowObject();
//    } catch (Exception ex) {
//      throw new KijiIOException(
//          "Error attempting to receive a table from the pool: " + mPool.toString(),
//          ex);
//    }
//  }
//
//  /** {@inheritDoc} */
//  @Override
//  public void close() throws IOException {
//    // When we're closed, close any saved tables. Assumes that no one is requesting/returning
//    // tables before close() returns.
//    try {
//      if (mPool.isClosed()) {
//        throw new KijiIOException("Attempting to close an already closed table pool.");
//      }
//      DebugResourceTracker.get().unregisterResource(this);
//      mPool.close();
//    } catch (Exception ex) {
//      throw new KijiIOException(
//          "Exception while attempting to close and clean-up pool: " + mPool.toString(),
//          ex);
//    }
//  }
//
//  /**
//   * Our implementation of a PooledObject factory for this class.  Implements a validate method to
//   * ensures that tables are valid before serving them.
//   *
//   * This class is package-private to permit testing.
//   */
//  class PooledHTableFactory extends BasePoolableObjectFactory<PooledHTable> {
//    @Override
//    public PooledHTable makeObject() throws Exception {
//      final HTableInterface hTable = mHTableFactory.create(mKiji.getConf(), mHBaseTableName);
//      LOG.debug("Creating a new pooled HTable: " + hTable.toString());
//      return new PooledHTable(hTable);
//    }
//
//    @Override
//    public void destroyObject(PooledHTable obj) throws Exception {
//      final HTableInterface hTable = obj.getWrappedTable();
//      LOG.debug("Destroying a pooled HTable: " + hTable.toString());
//      hTable.close();
//    }
//
//    @Override
//    public boolean validateObject(PooledHTable obj) {
//      try {
//        return obj.isValid();
//      } catch (IOException ioe) {
//        throw new KijiIOException("Could not confirm state of " + obj.toString(), ioe);
//      }
//    }
//  }
//
//  @Override
//  public String toString() {
//    return Objects.toStringHelper(this)
//        .add("kiji_uri", mKiji.getURI().toString())
//        .add("table_name", mTableName)
//        .add("table_count", mPool.getNumActive() + mPool.getNumIdle())
//        .toString();
//  }
//
//  /**
//   * A proxy class that implements HTableInterface.close method to return the
//   * wrapped table back to the table pool. This is virtually identical to HBase's
//   * implementation inside HTablePool, but adds an {@link #isValid} method.
//   *
//   * This class is package-private to permit testing.
//   */
//  class PooledHTable implements HTableInterface {
//    /** Actual underlying HTableInterface.  Must be an HTable. */
//    private HTableInterface mTable;
//
//    /**
//     * Constructor.
//     *
//     * @param table An HTableInterface. Should be an instance of HTable.
//     */
//    public PooledHTable(HTableInterface table) {
//      this.mTable = table;
//    }
//
//    /**
//     * Checks the state of the pooled table for whether it can still safely be used.
//     *
//     * @return true if the underlying connection of the HTable is still valid.
//     * @throws IOException If an exception occurs while trying to check the connection state.
//     */
//    public boolean isValid() throws IOException {
//      // TODO(SCHEMA-622): Note that this relies on deprecated functionality.  Revisit this when
//      // migrating functionality to the bridges.
//      final boolean connectionClosed = ((HTable)mTable).getConnection().isClosed();
//      if (connectionClosed) {
//        mTable.close();
//      }
//      return !connectionClosed;
//    }
//
//    @Override
//    public byte[] getTableName() {
//      return mTable.getTableName();
//    }
//
//    @Override
//    public Configuration getConfiguration() {
//      return mTable.getConfiguration();
//    }
//
//    @Override
//    public HTableDescriptor getTableDescriptor() throws IOException {
//      return mTable.getTableDescriptor();
//    }
//
//    @Override
//    public boolean exists(Get get) throws IOException {
//      return mTable.exists(get);
//    }
//
//    @Override
//    public void batch(List<? extends Row> actions, Object[] results) throws IOException,
//        InterruptedException {
//      mTable.batch(actions, results);
//    }
//
//    @Override
//    public Object[] batch(List<? extends Row> actions) throws IOException,
//        InterruptedException {
//      return mTable.batch(actions);
//    }
//
//    @Override
//    public Result get(Get get) throws IOException {
//      return mTable.get(get);
//    }
//
//    @Override
//    public Result[] get(List<Get> gets) throws IOException {
//      return mTable.get(gets);
//    }
//
//    @Override
//    @SuppressWarnings("deprecation")
//    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
//      return mTable.getRowOrBefore(row, family);
//    }
//
//    @Override
//    public ResultScanner getScanner(Scan scan) throws IOException {
//      return mTable.getScanner(scan);
//    }
//
//    @Override
//    public ResultScanner getScanner(byte[] family) throws IOException {
//      return mTable.getScanner(family);
//    }
//
//    @Override
//    public ResultScanner getScanner(byte[] family, byte[] qualifier)
//        throws IOException {
//      return mTable.getScanner(family, qualifier);
//    }
//
//    @Override
//    public void put(Put put) throws IOException {
//      mTable.put(put);
//    }
//
//    @Override
//    public void put(List<Put> puts) throws IOException {
//      mTable.put(puts);
//    }
//
//    @Override
//    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
//        byte[] value, Put put) throws IOException {
//      return mTable.checkAndPut(row, family, qualifier, value, put);
//    }
//
//    @Override
//    public void delete(Delete delete) throws IOException {
//      mTable.delete(delete);
//    }
//
//    @Override
//    public void delete(List<Delete> deletes) throws IOException {
//      mTable.delete(deletes);
//    }
//
//    @Override
//    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
//        byte[] value, Delete delete) throws IOException {
//      return mTable.checkAndDelete(row, family, qualifier, value, delete);
//    }
//
//    @Override
//    public Result increment(Increment increment) throws IOException {
//      return mTable.increment(increment);
//    }
//
//    @Override
//    public long incrementColumnValue(byte[] row, byte[] family,
//        byte[] qualifier, long amount) throws IOException {
//      return mTable.incrementColumnValue(row, family, qualifier, amount);
//    }
//
//    @Override
//    public long incrementColumnValue(byte[] row, byte[] family,
//        byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
//      return mTable.incrementColumnValue(row, family, qualifier, amount,
//          writeToWAL);
//    }
//
//    @Override
//    public boolean isAutoFlush() {
//      return mTable.isAutoFlush();
//    }
//
//    @Override
//    public void flushCommits() throws IOException {
//      mTable.flushCommits();
//    }
//
//    /**
//     * Returns the actual table back to the pool.
//     *
//     * @throws IOException if there was a problem returning to the pool.
//     */
//    public void close() throws IOException {
//      try {
//        mPool.returnObject(this);
//      } catch (Exception e) {
//        throw new KijiIOException("Error when returning a PoolHTable to its pool.", e);
//      }
//    }
//
//    @Override
//    public RowLock lockRow(byte[] row) throws IOException {
//      return mTable.lockRow(row);
//    }
//
//    @Override
//    public void unlockRow(RowLock rl) throws IOException {
//      mTable.unlockRow(rl);
//    }
//
//    @Override
//    public <T extends CoprocessorProtocol> T coprocessorProxy(
//        Class<T> protocol, byte[] row) {
//      return mTable.coprocessorProxy(protocol, row);
//    }
//
//    @Override
//    public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(
//        Class<T> protocol, byte[] startKey, byte[] endKey,
//        Batch.Call<T, R> callable) throws Throwable {
//      return mTable.coprocessorExec(protocol, startKey, endKey, callable);
//    }
//
//    @Override
//    public <T extends CoprocessorProtocol, R> void coprocessorExec(
//        Class<T> protocol, byte[] startKey, byte[] endKey,
//        Batch.Call<T, R> callable, Batch.Callback<R> callback)
//        throws Throwable {
//      mTable.coprocessorExec(protocol, startKey, endKey, callable, callback);
//    }
//
//    @Override
//    public String toString() {
//      return Objects.toStringHelper(this)
//          .add("Table", mTable)
//          .add("KijiURI", mKiji.getURI())
//          .toString();
//    }
//
//    /**
//     * Expose the wrapped HTable to tests in the same package.
//     *
//     * @return The wrapped htable.
//     */
//    HTableInterface getWrappedTable() {
//      return mTable;
//    }
//
//    @Override
//    public void mutateRow(RowMutations rm) throws IOException {
//      mTable.mutateRow(rm);
//    }
//
//    @Override
//    public Result append(Append append) throws IOException {
//      return mTable.append(append);
//    }
//
//    @Override
//    public void setAutoFlush(boolean autoFlush) {
//      mTable.setAutoFlush(autoFlush);
//    }
//
//    @Override
//    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
//      mTable.setAutoFlush(autoFlush, clearBufferOnFail);
//    }
//
//    @Override
//    public long getWriteBufferSize() {
//      return mTable.getWriteBufferSize();
//    }
//
//    @Override
//    public void setWriteBufferSize(long writeBufferSize) throws IOException {
//      mTable.setWriteBufferSize(writeBufferSize);
//    }
//  }
//}
