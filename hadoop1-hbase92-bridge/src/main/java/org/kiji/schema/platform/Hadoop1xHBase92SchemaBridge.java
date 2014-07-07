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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * Hadoop 1.x and HBase 0.92.x-backed implementation of the SchemaPlatformBridge API.
 */
@ApiAudience.Private
public final class Hadoop1xHBase92SchemaBridge extends SchemaPlatformBridge {
  private static final Logger LOG = LoggerFactory.getLogger(Hadoop1xHBase92SchemaBridge.class);

  /** {@inheritDoc} */
  @Override
  public void initializeHadoopResources() {
    // Do nothing: Configuration resources include hdfs/mapred-site/default.xml by default.
  }

  /** {@inheritDoc} */
  @Override
  public void setAutoFlush(HTableInterface hTable, boolean autoFlush) {
    // The HTable implementation of HTableInterface can do this; downcast if available.
    // setAutoFlush is not a member of HTableInterface in HBase 0.92.X
    if (hTable instanceof HTable) {
      ((HTable) hTable).setAutoFlush(autoFlush);
    } else {
      LOG.error("Cannot set autoFlush=" + autoFlush + " for HTableInterface impl "
          + hTable.getClass().getName());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void setWriteBufferSize(HTableInterface hTable, long bufSize)
      throws IOException {
    // The HTable implementation of HTableInterface can do this; downcast if available.
    // setWriteBufferSize is not a member of HTableInterface in HBase 0.92.X
    if (hTable instanceof HTable) {
      ((HTable) hTable).setWriteBufferSize(bufSize);
    } else {
      LOG.error("Cannot set writeBufSize=" + bufSize + " for HTableInterface impl "
          + hTable.getClass().getName());
    }
  }

  /** {@inheritDoc} */
  @Override
  public Put addKVToPut(Put put, KeyValue kv) throws IOException {
    return put.add(kv);
  }

  /** {@inheritDoc} */
  @Override
  public HFile.Writer createHFileWriter(Configuration conf,
      FileSystem fs, Path path, int blockSizeBytes, String compressionType)
      throws IOException {

     return HFile.getWriterFactory(conf, new CacheConfig(conf)).createWriter(
         fs, path, blockSizeBytes,
         Compression.getCompressionAlgorithmByName(compressionType),
         KeyValue.KEY_COMPARATOR);
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

  /** {@inheritDoc} */
  @Override
  public FamilyFilter createFamilyFilter(CompareFilter.CompareOp op, byte[] family) {
    return new FamilyFilter(op, new BinaryComparator(family));
  }

  /** {@inheritDoc} */
  @Override
  public QualifierFilter createQualifierFilter(CompareFilter.CompareOp op, byte[] qualifier) {
    return new QualifierFilter(op, new BinaryComparator(qualifier));
  }

  /** {@inheritDoc} */
  @Override
  public Delete createDelete(byte[] rowKey, long timestamp) {
    return new Delete(rowKey, timestamp, null);
  }

  /** {@inheritDoc} */
  @Override
  public int compareBloom(HColumnDescriptor col1, HColumnDescriptor col2) {
    return col1.getBloomFilterType().compareTo(col2.getBloomFilterType());
  }

  /** {@inheritDoc} */
  @Override
  public int compareCompression(HColumnDescriptor col1, HColumnDescriptor col2) {
    return col1.getCompressionType().toString().compareTo(col2.getCompressionType().toString());
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
        String compressionAlgorithm) {
      mHColumnDescriptor.setCompressionType(
          Compression.getCompressionAlgorithmByName(compressionAlgorithm));
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
    public HColumnDescriptorBuilderInterface setBloomType(String bloomType) {
      mHColumnDescriptor.setBloomFilterType(StoreFile.BloomType.valueOf(bloomType));
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public HColumnDescriptor build() {
      return new HColumnDescriptor(mHColumnDescriptor);
    }
  }
}

