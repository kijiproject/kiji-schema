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

package org.kiji.schema.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseEntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRegion;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.impl.hbase.HBaseKijiRowData;
import org.kiji.schema.impl.hbase.HBaseKijiTable;
import org.kiji.schema.util.ResourceUtils;

/** InputFormat for Hadoop MapReduce jobs reading from a Kiji table. */
@ApiAudience.Public
@ApiStability.Evolving
@Deprecated
public class KijiTableInputFormat
    extends InputFormat<EntityId, KijiRowData>
    implements Configurable {
  /** Configuration of this input format. */
  private Configuration mConf;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  public RecordReader<EntityId, KijiRowData> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new KijiTableRecordReader(mConf);
  }

  /** {@inheritDoc} */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    final Configuration conf = context.getConfiguration();
    final KijiURI inputTableURI = getInputTableURI(conf);
    final Kiji kiji = Kiji.Factory.open(inputTableURI, conf);
    final KijiTable table = kiji.openTable(inputTableURI.getTable());

    final HTableInterface htable = HBaseKijiTable.downcast(table).openHTableConnection();
    try {
      final List<InputSplit> splits = Lists.newArrayList();
      for (KijiRegion region : table.getRegions()) {
        final byte[] startKey = region.getStartKey();
        // TODO: a smart way to get which location is most relevant.
        final String location =
            region.getLocations().isEmpty() ? null : region.getLocations().iterator().next();
        final TableSplit tableSplit = new TableSplit(
            htable.getTableName(), startKey, region.getEndKey(), location);
        splits.add(new KijiTableSplit(tableSplit, startKey));
      }
      return splits;

    } finally {
      htable.close();
    }
  }

  /**
   * Configures a Hadoop M/R job to read from a given table.
   *
   * @param job Job to configure.
   * @param tableURI URI of the table to read from.
   * @param dataRequest Data request.
   * @param startRow Minimum row key to process.
   * @param endRow Maximum row Key to process.
   * @throws IOException on I/O error.
   */
  public static void configureJob(
      Job job,
      KijiURI tableURI,
      KijiDataRequest dataRequest,
      String startRow,
      String endRow)
      throws IOException {

    final Configuration conf = job.getConfiguration();
    // As a precaution, be sure the table exists and can be opened.
    final Kiji kiji = Kiji.Factory.open(tableURI, conf);
    final KijiTable table = kiji.openTable(tableURI.getTable());
    ResourceUtils.releaseOrLog(table);
    ResourceUtils.releaseOrLog(kiji);

    // TODO: Check for jars config:
    // GenericTableMapReduceUtil.initTableInput(hbaseTableName, scan, job);

    // TODO: Obey specified start/end rows.

    // Write all the required values to the job's configuration object.
    job.setInputFormatClass(KijiTableInputFormat.class);
    final String serializedRequest =
        Base64.encodeBase64String(SerializationUtils.serialize(dataRequest));
    conf.set(KijiConfKeys.INPUT_DATA_REQUEST, serializedRequest);
    conf.set(KijiConfKeys.INPUT_TABLE_URI, tableURI.toString());
  }

  /** Hadoop record reader for Kiji table rows. */
  public static class KijiTableRecordReader
      extends RecordReader<EntityId, KijiRowData> {

    /** Data request. */
    protected final KijiDataRequest mDataRequest;

    /** Hadoop Configuration object containing settings. */
    protected final Configuration mConf;

    private Kiji mKiji = null;
    private KijiTable mTable = null;
    private KijiTableReader mReader = null;
    private KijiRowScanner mScanner = null;
    private Iterator<KijiRowData> mIterator = null;

    private KijiTableSplit mSplit = null;

    private HBaseKijiRowData mCurrentRow = null;

    /**
     * Creates a new RecordReader for this input format. This RecordReader will perform the actual
     * reads from Kiji.
     *
     * @param conf The configuration object for this Kiji.
     */
    public KijiTableRecordReader(Configuration conf) {
      mConf = conf;

      // Get data request from the job configuration.
      final String dataRequestB64 = checkNotNull(mConf.get(KijiConfKeys.INPUT_DATA_REQUEST),
          "Missing data request in job configuration.");
      final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(dataRequestB64));
      mDataRequest = (KijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);
    }

    /** {@inheritDoc} */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      assert split instanceof KijiTableSplit;
      mSplit = (KijiTableSplit) split;

      final Configuration conf = context.getConfiguration();
      final KijiURI inputURI = getInputTableURI(conf);
      mKiji = Kiji.Factory.open(inputURI, conf);
      mTable = mKiji.openTable(inputURI.getTable());
      mReader = mTable.openTableReader();
      final KijiScannerOptions scannerOptions =
          new KijiScannerOptions()
          .setStartRow(HBaseEntityId.fromHBaseRowKey(mSplit.getStartRow()))
          .setStopRow(HBaseEntityId.fromHBaseRowKey(mSplit.getEndRow()));
      mScanner = mReader.getScanner(mDataRequest, scannerOptions);
      mIterator = mScanner.iterator();
      mCurrentRow = null;
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getCurrentKey() throws IOException {
      return mCurrentRow.getEntityId();
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData getCurrentValue() throws IOException {
      return mCurrentRow;
    }

    /** {@inheritDoc} */
    @Override
    public float getProgress() throws IOException {
      // TODO: Implement
      return 0.0f;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws IOException {
      if (mIterator.hasNext()) {
        mCurrentRow = (HBaseKijiRowData) mIterator.next();
        return true;
      } else {
        mCurrentRow = null;
        return false;
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      ResourceUtils.closeOrLog(mScanner);
      ResourceUtils.closeOrLog(mReader);
      ResourceUtils.releaseOrLog(mTable);
      ResourceUtils.releaseOrLog(mKiji);
      mIterator = null;
      mScanner = null;
      mReader = null;
      mTable = null;
      mKiji = null;

      mSplit = null;
      mCurrentRow = null;
    }
  }

  /**
   * Reports the URI of the configured input table.
   *
   * @param conf Read the input URI from this configuration.
   * @return the configured input URI.
   * @throws IOException on I/O error.
   */
  private static KijiURI getInputTableURI(Configuration conf) throws IOException {
    try {
      return KijiURI.newBuilder(conf.get(KijiConfKeys.INPUT_TABLE_URI)).build();
    } catch (KijiURIException kue) {
      throw new IOException(kue);
    }
  }
}
