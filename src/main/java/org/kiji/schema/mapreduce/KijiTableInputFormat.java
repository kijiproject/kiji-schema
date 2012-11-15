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
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestException;
import org.kiji.schema.KijiManagedHBaseTableName;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.SpecificCellDecoderFactory;
import org.kiji.schema.impl.DefaultHTableInterfaceFactory;
import org.kiji.schema.impl.HBaseDataRequestAdapter;
import org.kiji.schema.impl.HBaseKijiRowData;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.impl.HBaseTableRecordReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.TableLayoutSerializer;

/**
 * An InputFormat for MapReduce jobs over Kiji tables. Use the KijiTableInputFormat#setOptions
 * method to configure this input format for use in a mapreduce job.
 */
public class KijiTableInputFormat
    extends InputFormat<EntityId, KijiRowData>
    implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTableInputFormat.class);

  /** Configuration variable for the data request. */
  public static final String REQUEST_CONF_NAME = "kiji.input.request";

  /** Configuration variable for the input kiji instance. */
  public static final String INSTANCE_CONF_NAME = "kiji.input.instance";

  /** Configuration variable for the input kiji table. */
  public static final String TABLE_CONF_NAME = "kiji.input.table";

  /** Configuration of this input format. */
  private Configuration mConf;

  private HTableInputFormat mDelegate = new HTableInputFormat();

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mDelegate.setConf(conf);

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
    final String instance = conf.get(INSTANCE_CONF_NAME);
    final String table = conf.get(TABLE_CONF_NAME);

    // Get the name of the HBase table that stores the Kiji table data.
    final String hbaseTableName = KijiManagedHBaseTableName
        .getKijiTableName(instance, table)
        .toString();

    // Get the start keys for all regions, and associate these with the user's splits.
    final HTable htable = new HTable(context.getConfiguration(), hbaseTableName);
    Pair<byte[][], byte[][]> startEndKeys = null;
    try {
      startEndKeys = htable.getStartEndKeys();
    } finally {
      IOUtils.closeQuietly(htable);
    }
    final byte[][] startKeys = startEndKeys.getFirst();
    final byte[][] endKeys = startEndKeys.getSecond();

    // Get HBase TableSplits.
    final List<InputSplit> splits = mDelegate.getSplits(context);

    // Translate HBase TableSplit instances into KijiTableSplit instances.
    final List<InputSplit> outSplits = Lists.newArrayList();
    int startKeyIdx = 0;
    for (InputSplit inSplit : splits) {
      final TableSplit tblSplit = (TableSplit) inSplit;
      final byte[] splitStart = tblSplit.getStartRow();

      checkState(startKeyIdx < startKeys.length);

      // Look up the region start key for the region represented by this
      // TableSplit. This routine assumes the TableSplits are ordered
      // by start keys.
      byte[] regionStart = null;
      if (splitStart.length == 0) {
        // This split starts at the beginning of the table.
        regionStart = startKeys[0];
        startKeyIdx = 1; // Next split is the next region.
      } else if (Bytes.compareTo(splitStart, startKeys[startKeyIdx]) == 0) {
        // This split starts on an aligned region boundary.
        regionStart = splitStart;
        startKeyIdx++; // Next split starts at next region boundary.
      } else {
        // We need to find the associated region in the scan.
        for (startKeyIdx = 0; startKeyIdx < startKeys.length; startKeyIdx++) {
          if (endKeys[startKeyIdx].length == 0
              || Bytes.compareTo(endKeys[startKeyIdx], splitStart) > 0) {
            // This region is the first that ends after the split.
            // It should be the case that it starts at or before the split.
            checkState(Bytes.compareTo(startKeys[startKeyIdx], splitStart) <= 0);
            break;
          }
        }

        if (startKeyIdx == startKeys.length) {
          // We couldn't find the region for this split. This suggests the
          // table has some issues.
          throw new IOException("Couldn't find region start key for split with start key "
              + Bytes.toString(splitStart));
        }

        regionStart = startKeys[startKeyIdx++];
      }

      outSplits.add(new KijiTableSplit(tblSplit, regionStart));
    }

    return outSplits;
  }

  /**
   * Gets a table layout for the specified Kiji table.
   *
   * @param conf The configuration object specifying the desired Kiji instance.
   * @param tableName The name of the table of the desired layout.
   * @return The desired table layout.
   * @throws IOException if there is an error reading the table Layout.
   */
  private static KijiTableLayout getTableLayout(KijiConfiguration conf, String tableName)
      throws IOException {
    Kiji kiji = null;
    KijiTable table = null;
    KijiTableLayout layout = null;

    try {
      kiji = Kiji.open(conf);
      table = kiji.openTable(tableName);
      layout = HBaseKijiTable.downcast(table).getLayout();
    } finally {
      IOUtils.closeQuietly(table);
      IOUtils.closeQuietly(kiji);
    }

    return layout;
  }

  /**
   * Serializes the specified data request and address into the specified job's configuration.
   *
   * @param job Job to configure.
   * @param table Name of the Kiji table to read from.
   * @param request The data requested.
   * @throws IOException if an error occurs during serialization.
   */
  public static void setOptions(String table, KijiDataRequest request, Job job)
      throws IOException {
    setOptions(job, KijiConfiguration.DEFAULT_INSTANCE_NAME, table, request);
  }

  /**
   * Serializes the specified data request and address into the specified job's configuration.
   *
   * @param job Job to configure.
   * @param table Name of the Kiji table to read from.
   * @param request The data requested.
   * @param startRow The minimum row key to process.
   * @param endRow The maximum row Key to process.
   * @throws IOException if an error occurs during serialization.
   */
  public static void setOptions(Job job, String table, KijiDataRequest request, String startRow,
      String endRow) throws IOException {
    setOptions(job, KijiConfiguration.DEFAULT_INSTANCE_NAME, table, request, startRow, endRow);
  }

  /**
   * Serializes the specified data request and address into the specified job's configuration.
   *
   * @param job Job to configure.
   * @param instance Name of the Kiji instance to read from.
   * @param table Name of the Kiji table to read from.
   * @param request The data requested.
   * @throws IOException if an error occurs during serialization.
   */
  public static void setOptions(Job job, String instance, String table, KijiDataRequest request)
      throws IOException {
    setOptions(job, instance, table, request, "", "");
  }

  /**
   * Serializes the specified data request and address into the specified job's configuration.
   *
   * @param job Job to configure.
   * @param instance Name of the Kiji instance to read from.
   * @param table Name of the Kiji table to read from.
   * @param request The data requested.
   * @param startRow The minimum row key to process.
   * @param endRow The maximum row Key to process.
   * @throws IOException if an error occurs during serialization.
   */
  public static void setOptions(Job job, String instance, String table, KijiDataRequest request,
      String startRow, String endRow) throws IOException {
    final Configuration conf = job.getConfiguration();
    final KijiConfiguration kijiConf = new KijiConfiguration(conf, instance);

    // Put the table layout in the configuration object.
    final KijiTableLayout layout = getTableLayout(kijiConf, table);
    TableLayoutSerializer.serializeInputTableLayout(layout, conf);

    // Get the name of the HBase table that stores the Kiji table data.
    String hbaseTableName = KijiManagedHBaseTableName
        .getKijiTableName(instance, table)
        .toString();

    try {
      // Create the HBase scan configured to read the appropriate input from the Kiji table.
      Scan scan = new HBaseDataRequestAdapter(request).toScan(layout);

      final EntityIdFactory eidFactory = EntityIdFactory.create(layout.getDesc().getKeysFormat());

      // Apply the start & end rows if specified.
      if (!startRow.isEmpty()) {
        EntityId startEntity = eidFactory.fromKijiRowKey(startRow);
        scan.setStartRow(startEntity.getHBaseRowKey());
      }
      if (!endRow.isEmpty()) {
        System.out.println("END ROW KEY : " + endRow);
        EntityId limitEntity = eidFactory.fromKijiRowKey(endRow);
        scan.setStopRow(limitEntity.getHBaseRowKey());
      }

      // Configure the parent table input format using HBase.
      GenericTableMapReduceUtil.initTableInput(hbaseTableName, scan, job);
    } catch (KijiDataRequestException e) {
      throw new IOException(e);
    }

    // Write all the required values to the job's configuration object.
    final String serializedRequest = Base64
        .encodeBase64String(SerializationUtils.serialize(request));
    conf.set(REQUEST_CONF_NAME, serializedRequest);
    conf.set(INSTANCE_CONF_NAME, instance);
    conf.set(TABLE_CONF_NAME, table);
  }

  /**
   * Record reader that KijiTableInputFormat uses.
   */
  public static class KijiTableRecordReader
      extends RecordReader<EntityId, KijiRowData> {
    /** Kiji instance to read from. */
    protected final String mInstance;

    /** Kiji table to read from. */
    protected final String mTable;

    /** The data request. */
    protected final KijiDataRequest mRequest;

    /** Hadoop Configuration object containing settings. */
    protected final Configuration mConf;

    /** True if paging has been enabled in mRequest. */
    protected final boolean mPagingEnabled;

    private HBaseTableRecordReader mDelegate;
    private KijiTableLayout mLayout;
    private KijiCellDecoderFactory mCellDecoderFactory;
    private Kiji mKiji;
    private HBaseKijiTable mKijiTable;
    private HBaseKijiRowData mCurrentRow;

    /**
     * Creates a new RecordReader for this input format. This RecordReader will perform the actual
     * reads from Kiji.
     *
     * @param conf The configuration object for this Kiji.
     */
    public KijiTableRecordReader(Configuration conf) {
      mConf = conf;

      // Get data request from the job configuration.
      final String serializedRequest = checkNotNull(mConf.get(REQUEST_CONF_NAME, null),
          "Missing data request in job configuration.");
      final byte[] decodedRequest = Base64.decodeBase64(
          serializedRequest.getBytes(Charset.forName("UTF-8")));
      mRequest = (KijiDataRequest) SerializationUtils.deserialize(decodedRequest);
      mPagingEnabled = mRequest.isPagingEnabled();

      // Get input Kiji instance and table from the job configuration.
      mInstance = checkNotNull(mConf.get(INSTANCE_CONF_NAME),
          "Missing input Kiji instance in job configuration.");
      mTable = checkNotNull(mConf.get(TABLE_CONF_NAME, null),
          "Missing input Kiji table in job configuration.");
    }

    /** {@inheritDoc} */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws InterruptedException, IOException {

      assert split instanceof KijiTableSplit;

      // Get the table layout from the configuration object.
      mLayout = TableLayoutSerializer.readInputTableLayout(mConf);

      // Get a kiji cell decoder factory.
      mKiji = new Kiji(
          new KijiConfiguration(mConf, mInstance),
          false,
          DefaultHTableInterfaceFactory.get());
      mKijiTable = HBaseKijiTable.downcast(mKiji.openTable(mTable));
      mCellDecoderFactory = new SpecificCellDecoderFactory(mKiji.getSchemaTable());

      // Get the HBase scan object from the configuration object.
      final Scan scan =
          GenericTableMapReduceUtil.convertStringToScan(mConf.get(HTableInputFormat.SCAN));
      scan.setStartRow(((KijiTableSplit) split).getStartRow())
          .setStopRow(((KijiTableSplit) split).getEndRow());

      // Get the HTable to write to.
      final HTableInterface htable = mKijiTable.getHTable();

      // Create the table record reader.
      mDelegate = new HBaseTableRecordReader(mLayout.getDesc().getKeysFormat());
      mDelegate.setScan(scan);
      // TODO: TableRecordReader requires type HTable, instead of HTableInterface:
      mDelegate.setHTable((HTable) htable);
      mDelegate.init();
      mDelegate.initialize(split, context);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getCurrentKey() throws InterruptedException, IOException {
      return mCurrentRow.getEntityId();
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData getCurrentValue() throws InterruptedException, IOException {
      return mCurrentRow;
    }

    /** {@inheritDoc} */
    @Override
    public float getProgress() throws InterruptedException, IOException {
      return mDelegate.getProgress();
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws InterruptedException, IOException {
      if (mDelegate.nextKeyValue()) {
        // Build the HBaseKijiRowData object.
        final Result hbaseValue = mDelegate.getCurrentValue();
        final HBaseKijiRowData.Options standard = new HBaseKijiRowData.Options()
            .withDataRequest(mRequest)
            .withTableLayout(mLayout)
            .withHBaseResult(hbaseValue)
            .withCellDecoderFactory(mCellDecoderFactory);

        if (mPagingEnabled) {
          mCurrentRow = new HBaseKijiRowData(standard.withHTable(mKijiTable.getHTable()));
        } else {
          mCurrentRow = new HBaseKijiRowData(standard);
        }

        return true;
      } else {
        return false;
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mDelegate.close();
      IOUtils.closeQuietly(mKiji);
      IOUtils.closeQuietly(mKijiTable);
    }
  }
}
