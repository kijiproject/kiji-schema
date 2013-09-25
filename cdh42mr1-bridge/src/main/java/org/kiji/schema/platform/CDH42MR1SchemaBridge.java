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
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.mapred.JobConf;

import org.kiji.annotations.ApiAudience;

/**
 * CDH4.2-backed implementation of the SchemaPlatformBridge API.
 */
@ApiAudience.Private
public final class CDH42MR1SchemaBridge extends SchemaPlatformBridge {
  /** {@inheritDoc} */
  @Override
  public void initializeHadoopResources() {
    // Force initialization of HdfsConfiguration,
    // to register hdfs-site.xml and hdfs-default.xml resources:
    HdfsConfiguration.init();

    // Force initialization of JobConf,
    // to register mapred-site.xml and mapred-default.xml resources:
    try {
      // JobConf does not provide a static initialization method,
      // use Class.forName() to trigger static initialization:
      Class.forName(JobConf.class.getName());
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(
          "Error initializing class JobConf to register mapred resources.", cnfe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void setAutoFlush(HTableInterface hTable, boolean autoFlush) {
    // We can do this directly in CDH4.
    hTable.setAutoFlush(autoFlush);
  }

  /** {@inheritDoc} */
  @Override
  public void setWriteBufferSize(HTableInterface hTable, long bufSize)
      throws IOException {
    // We can do this directly in CDH4.
    hTable.setWriteBufferSize(bufSize);
  }

  /** {@inheritDoc} */
  @Override
  public HFile.Writer createHFileWriter(Configuration conf,
      FileSystem fs, Path path, int blockSizeBytes, Compression.Algorithm compressionType,
      KeyComparator comparator) throws IOException {

     return HFile.getWriterFactory(conf, new CacheConfig(conf))
         .withPath(fs, path)
         .withBlockSize(blockSizeBytes)
         .withCompression(compressionType)
         .withComparator(comparator)
         .create();
  }

  /** {@inheritDoc} */
  @Override
  public HColumnDescriptorBuilderInterface createHColumnDescriptorBuilder() {
    return new HColumnDescriptorBuilder();
  }

  /** {@inheritDoc} */
  @Override
  public HColumnDescriptorBuilderInterface createHColumnDescriptorBuilder(byte[] family) {
    return new HColumnDescriptorBuilder(family);
  }

  /**
   * Platform-specific implementation of HColumnDescriptorBuilderInterface.
   */
  public static class HColumnDescriptorBuilder implements HColumnDescriptorBuilderInterface {
    private HColumnDescriptor mHColumnDescriptor;

    /**
     * Construct a new HColumnDescriptorBuilder.
     */
    HColumnDescriptorBuilder() {
      mHColumnDescriptor = new HColumnDescriptor();
    }

    /**
     * Construct a new HColumnDescriptorBuilder with the family specified for the
     * HColumnDescriptor.
     *
     * @param family Family for the HColumnDescriptor.
     */
    HColumnDescriptorBuilder(byte[] family) {
      mHColumnDescriptor = new HColumnDescriptor(family);
    }

    /** {@inheritDoc} */
    @Override
    public HColumnDescriptorBuilderInterface setMaxVersions(int maxVersions) {
      mHColumnDescriptor.setMaxVersions(maxVersions);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public HColumnDescriptorBuilderInterface setCompressionType(
        Compression.Algorithm compressionAlgorithm) {
      mHColumnDescriptor.setCompressionType(compressionAlgorithm);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public HColumnDescriptorBuilderInterface setInMemory(boolean inMemory) {
      mHColumnDescriptor.setInMemory(inMemory);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public HColumnDescriptorBuilderInterface setBlockCacheEnabled(boolean blockCacheEnabled) {
      mHColumnDescriptor.setBlockCacheEnabled(blockCacheEnabled);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public HColumnDescriptorBuilderInterface setTimeToLive(int timeToLive) {
      mHColumnDescriptor.setTimeToLive(timeToLive);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public HColumnDescriptorBuilderInterface setBloomType(StoreFile.BloomType bloomType) {
      mHColumnDescriptor.setBloomFilterType(bloomType);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public HColumnDescriptor build() {
      return new HColumnDescriptor(mHColumnDescriptor);
    }
  }
}

