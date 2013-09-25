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
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import org.kiji.annotations.ApiAudience;
import org.kiji.delegation.Lookups;

/**
 * Abstract representation of an underlying platform for KijiSchema. This interface
 * is fulfilled by specific implementation providers that are dynamically chosen
 * at runtime based on the Hadoop &amp; HBase jars available on the classpath.
 */
@ApiAudience.Framework
public abstract class SchemaPlatformBridge {
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
   * Creates an HFile writer.
   *
   * @param conf the current configuration.
   * @param fs the Filesystem to write to.
   * @param path to the file to open for write access.
   * @param blockSizeBytes the block size to write within the HFile.
   * @param compressionType to use in the HFile
   * @param comparator the Key comparator to use.
   * @return a newly-opened HFile writer object.
   * @throws IOException if there's an error opening the file.
   */
  public abstract HFile.Writer createHFileWriter(
      Configuration conf,
      FileSystem fs, Path path, int blockSizeBytes,
      Compression.Algorithm compressionType,
      KeyComparator comparator)
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
     * @return This builder with the compressionType set.
     */
    HColumnDescriptorBuilderInterface setCompressionType(
        Compression.Algorithm compressionAlgorithm);

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
     * @return This builder with the bloomType set.
     */
    HColumnDescriptorBuilderInterface setBloomType(StoreFile.BloomType bloomType);

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
  public static final synchronized SchemaPlatformBridge get() {
    if (null != mBridge) {
      return mBridge;
    }
    mBridge = Lookups.getPriority(SchemaPlatformBridgeFactory.class).lookup().getBridge();
    return mBridge;
  }
}

