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

package org.kiji.schema.platform;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.delegation.Lookups;

/**
 * Abstract representation of an underlying platform for KijiSchema. This interface
 * is fulfilled by specific implementation providers that are dynamically chosen
 * at runtime based on the Hadoop &amp; HBase jars available on the classpath.
 */
@ApiAudience.Framework
public abstract class SchemaPlatformBridge {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaPlatformBridge.class);

  /**
   * This API should only be implemented by other modules within KijiSchema;
   * to discourage external users from extending this class, keep the c'tor
   * package-private.
   */
  SchemaPlatformBridge() {
  }

  /**
   * Initialize Hadoop configuration resources (HDFS and MapReduce resource files).
   *
   * Ensure that these configuration resources are loaded into subsequently-constructed
   * Configuration objects.
   * This includes hdfs-site.xml, hdfs-default.xml, mapred-site.xml and mapred-default.xml.
   */
  public abstract void initializeHadoopResources();

  /**
   * Set the autoflush behavior associated with an HTableInterface.
   *
   * @param hTable the table connection to configure.
   * @param autoFlush the auto-flush parameter.
   */
  public abstract void setAutoFlush(HTableInterface hTable, boolean autoFlush);

  /**
   * Set the size of the write buffer associated with an HTableInterface.
   *
   * @param hTable the table connection to configure.
   * @param bufSize the buffer size to set.
   * @throws IOException if there's an error setting the buffer size.
   */
  public abstract void setWriteBufferSize(HTableInterface hTable, long bufSize)
      throws IOException;


  /**
   * Adds an hbase KeyValue to a Put in a platform-safe way.
   *
   * @param put The Put object to receive the kv.
   * @param kv A KeyValue to add.
   * @throws IOException if there's an error while adding the keyvalue.
   * @return The amended Put object.
   */
  public abstract Put addKVToPut(Put put, KeyValue kv) throws IOException;

  /**
   * Creates an HFile writer.
   *
   * @param conf the current configuration.
   * @param fs the Filesystem to write to.
   * @param path to the file to open for write access.
   * @param blockSizeBytes the block size to write within the HFile.
   * @param compressionType to use in the HFile.
   *     Should be one of "lzo", "gzip", "none", "snappy", or "lz4".
   * @return a newly-opened HFile writer object.
   * @throws IOException if there's an error opening the file.
   */
  public abstract HFile.Writer createHFileWriter(
      Configuration conf,
      FileSystem fs, Path path, int blockSizeBytes,
      String compressionType)
      throws IOException;

  /**
   * Gets a builder for an HColumnDescriptor.
   *
   * @return a builder for an HColumnDescriptor.
   */
  public abstract HColumnDescriptorBuilderInterface createHColumnDescriptorBuilder();

  /**
   * Gets a builder for an HColumnDescriptor for a family.
   *
   * @param family of the HColumnDescriptor.
   * @return a builder for an HColumnDescriptor.
   */
  public abstract HColumnDescriptorBuilderInterface createHColumnDescriptorBuilder(byte[] family);

  /**
   * Gets a FamilyFilter for this version of HBase. Exists in the bridge because of
   * incompatible changes to BinaryComparator.
   *
   * @param op The comparator operator to use.
   * @param family The HBase family as bytes.
   * @return A family filter.
   */
  public abstract FamilyFilter createFamilyFilter(CompareFilter.CompareOp op, byte[] family);

  /**
   * Gets a QualifierFilter for this version of HBase. Exists in the bridge because of
   * incompatible changes to BinaryComparator.
   *
   * @param op The comparator operator to use.
   * @param qualifier The HBase qualifier as bytes.
   * @return A qualifier filter.
   */
  public abstract QualifierFilter createQualifierFilter(
      CompareFilter.CompareOp op,
      byte[] qualifier);

  /**
   * Gets a regex-based QualifierFilter for this version of HBase. Exists in the bridge
   * because of incompatible changes to RegexComparator.
   *
   * @param op The comparator operator to use.
   * @param regexString The regex to use for comparison, in string format.
   * @return A qualifier filter.
   */
  public abstract QualifierFilter createQualifierFilterFromRegex(
      CompareFilter.CompareOp op,
      String regexString);

  /**
   * Gets a regex-based RowFilter for this version of HBase. Exists in the bridge because of
   * incompatible changes to RegexComparator.
   *
   * @param op The comparator operator to use.
   * @param regexString The regex to use for comparison, in string format.
   * @return A row filter.
   */
  public abstract RowFilter createRowFilterFromRegex(
      CompareFilter.CompareOp op,
      String regexString);

  /**
   * Generates informative debug strings for a compare filter. Exists in the bridge because of
   * incompatible changes to WritableBytesComparable.
   *
   * @param cfilter A compare filter.
   * @return A two element array. The first element will be a String representation
   *     of the compare operator. The second element will be a String representation
   *     of the value compared against.
   */
  public abstract String[] debugStringsForCompareFilter(CompareFilter cfilter);

  /**
   * Creates a Delete operation for an entire row up to a particular timestamp. Necessary due to
   * the deprecation of row lock.
   *
   * @param rowKey The rowkey.
   * @param timestamp The maximum timestamp to delete up to.
   * @return A Delete operation.
   */
  public abstract Delete createDelete(byte[] rowKey, long timestamp);

  /**
   * Compares two column descriptors bloom type. Necessary due to different enum classpaths
   * between versions.
   *
   * @param col1 The first column descriptor.
   * @param col2 The second column descriptor.
   * @return 0 if the bloom settings are the same. Non-zero otherwise.
   */
  public abstract int compareBloom(HColumnDescriptor col1, HColumnDescriptor col2);

  /**
   * Compares two column descriptors compression settings. This is necessary due to
   * repackaging of compression related classes.
   *
   * @param col1 The first column descriptor.
   * @param col2 The second column descriptor.
   * @return 0 if the compression settings are the same. Non-zero otherwise.
   */
  public abstract int compareCompression(HColumnDescriptor col1, HColumnDescriptor col2);

  /**
   * Creates a UserPermission. This is necessary because due to changes in the type
   * of the hTableName parameter.
   *
   * @param user The user.
   * @param tableName The table.
   * @param family The family. May be null in which case the action is granted across the table.
   * @param actions The actions to grant.
   * @return A constructed UserPermission object.
   */
  public UserPermission createUserPermission(
      byte[] user,
      byte[] tableName,
      byte[] family,
      Action... actions
  ) {
    throw new UnsupportedOperationException(
        "Active SchemaPlatformBridge implementation does not support security operations.");
  }

  /**
   * Gets the KeyValues from an HBase Result.  Returns an empty array if the result has no Cells
   * in it.  This method may be slow for implementations for HBase 0.96+,
   * because it has to do an array copy.
   *
   * @param result The Result to get KeyValues from.
   * @return The KeyValues in result.
   */
  public abstract KeyValue[] keyValuesFromResult(Result result);

  /**
   * An interface for HColumnDescriptors, implemented by the bridges.
   */
  public interface HColumnDescriptorBuilderInterface {
    /**
     * Sets the maxVersions on the HColumnDescriptor.
     *
     * @param maxVersions to set.
     * @return This builder with the maxVersions set.
     */
    HColumnDescriptorBuilderInterface setMaxVersions(int maxVersions);

    /**
     * Sets the compression type on the HColumnDescriptor.
     *
     * @param compressionAlgorithm to set the compression type to.
     *      Should be one of 'none', 'gz', 'lz4', 'lzo', 'snappy'.
     * @return This builder with the compressionType set.
     */
    HColumnDescriptorBuilderInterface setCompressionType(
        String compressionAlgorithm);

    /**
     * Sets the inMemory flag on the HColumnDescriptor.
     *
     * @param inMemory whether column data should be kept in memory.
     * @return This builder with the inMemory flag set.
     */
    HColumnDescriptorBuilderInterface setInMemory(boolean inMemory);

    /**
     * Sets whether block cache is enabled on the HColumnDescriptor.
     *
     * @param blockCacheEnabled whether the block cache should be enabled on the HColumnDescriptor.
     * @return This builder with the blockCacheEnabled flag set.
     */
    HColumnDescriptorBuilderInterface setBlockCacheEnabled(boolean blockCacheEnabled);

    /**
     * Sets the timeToLive in the HColumnDescriptor describing the time to live for the column.
     *
     * @param timeToLive to set in the HColumnDescriptor.
     * @return This builder with timeToLive set.
     */
    HColumnDescriptorBuilderInterface setTimeToLive(int timeToLive);

    /**
     * Sets the bloom type used in the HColumnDescriptor.
     *
     * @param bloomType to set in the HColumnDescriptor.
     *     Must be one of "NONE", "ROW", or "ROWCOL".
     * @return This builder with the bloomType set.
     */
    HColumnDescriptorBuilderInterface setBloomType(String bloomType);

    /**
     * Returns the HColumnDescriptor.
     *
     * @return the HColumnDescriptor with all settings set.
     */
    HColumnDescriptor build();
  }

  private static SchemaPlatformBridge mBridge;

  /**
   * @return the SchemaPlatformBridge implementation appropriate to the current runtime
   *     conditions.
   */
  public static final SchemaPlatformBridge get() {
    if (null != mBridge) {
      return mBridge;
    }
    synchronized (SchemaPlatformBridge.class) {
      if (null == mBridge) {
        String hadoopVer = org.apache.hadoop.util.VersionInfo.getVersion();
        String hbaseVer = org.apache.hadoop.hbase.util.VersionInfo.getVersion();

        LOG.info("Loading bridge for Hadoop version {} and HBase version {}", hadoopVer, hbaseVer);
        final SchemaPlatformBridgeFactory factory =
            Lookups.getPriority(SchemaPlatformBridgeFactory.class).lookup();
        if (null == factory) {
          throw new RuntimeException(
              "Could not find suitable SchemaPlatformBridgeFactory. This may be an issue with"
              + " bridge providers on your classpath.");
        } else {
          LOG.info("Selected bridge: {}", factory.getClass().getName());
        }
        mBridge = factory.getBridge();
      }
      return mBridge;
    }
  }
}

