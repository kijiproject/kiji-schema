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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.avro.BloomType;
import org.kiji.schema.avro.LocalityGroupDesc;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/** This test validates some admin operations that {@link HBaseKiji} is responsible for. */
public class IntegrationTestHBaseKijiLayoutAdmin extends AbstractKijiIntegrationTest {
  private static final String FULL_FEATURED_TABLE_NAME = "user";
  private static final String FULL_FEATURED_TABLE_FAMILY_NAME = "info";
  private static final String FULL_FEATURED_TABLE_QUALIFIER_NAME = "name";
  private static final String SIMPLE_TABLE_NAME = "table";
  private static final String SIMPLE_TABLE_FAMILY_NAME = "family";
  private static final String SIMPLE_TABLE_QUALIFIER_NAME = "column";

  // These value match what's defined in full-featured-layout.json
  private static final long EXPECTED_MAX_FILESIZE = 10737418240L;
  private static final long EXPECTED_MEMSTORE_FLUSHSIZE = 268435456L;
  private static final long EXPECTED_BLOCKSIZE = 64;

  private Kiji mKiji;

  @Before
  public void setUp() throws Exception {
    mKiji = Kiji.Factory.get().open(getKijiURI());
  }

  @After
  public void tearDown() throws Exception {
    mKiji.release();
  }

  /**
   * Tests the creation of a table that uses all of the layout-1.2.0 hbase attributes.
   */
  @Test
  public void testHBaseAttributesWithFullFeaturedTable() throws IOException {
    // Create the table
    TableLayoutDesc fullFeaturedLayout = KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED);
    mKiji.createTable(fullFeaturedLayout);

    // Get the table descriptor
    HTableDescriptor hTableDescriptor = getHbaseTableDescriptor(FULL_FEATURED_TABLE_NAME);

    // Check the max file size value
    long actualMaxfilesize = hTableDescriptor.getMaxFileSize();
    String message = String.format("max_filesize should match the value %d defined in %s",
      EXPECTED_MAX_FILESIZE, KijiTableLayouts.FULL_FEATURED);
    assertEquals(message, EXPECTED_MAX_FILESIZE, actualMaxfilesize);

    // Check the memstore flush size value
    long actualMemstoreFlushsize = hTableDescriptor.getMemStoreFlushSize();
    message = String.format("memstore_flushsize should match the value %d defined in %s",
      EXPECTED_MEMSTORE_FLUSHSIZE, KijiTableLayouts.FULL_FEATURED);
    assertEquals(message, EXPECTED_MEMSTORE_FLUSHSIZE, actualMemstoreFlushsize);

    ColumnNameTranslator columnNameTranslator =
      new ColumnNameTranslator(KijiTableLayout.newLayout(fullFeaturedLayout));
    HBaseColumnName columnName = columnNameTranslator.toHBaseColumnName(
      new KijiColumnName(FULL_FEATURED_TABLE_FAMILY_NAME, FULL_FEATURED_TABLE_QUALIFIER_NAME));

    // Check the block size value
    HColumnDescriptor columnDescriptor = hTableDescriptor.getFamily(columnName.getFamily());
    int actualBlockSize = columnDescriptor.getBlocksize();
    message = String.format("block_size should match the value %d defined in %s",
      EXPECTED_BLOCKSIZE, KijiTableLayouts.FULL_FEATURED);
    assertEquals(message, EXPECTED_BLOCKSIZE, actualBlockSize);

    // Check the bloom type value
    StoreFile.BloomType actualBloomFilterType = columnDescriptor.getBloomFilterType();
    message = String.format("bloom_type should match the value %s defined in %s",
      BloomType.ROW, KijiTableLayouts.FULL_FEATURED);
    assertEquals(message, StoreFile.BloomType.ROW, actualBloomFilterType);
  }

  /**
   * This tests the update of hbase attributes on a table layout that already has values
   * set for those (the first version is using a programmatically modified version
   * of full-featured-layout.json).
   */
  @Test
  public void testUpdateTableLayoutHBaseAttributes() throws IOException {
    // Create the table
    TableLayoutDesc fullFeaturedLayout = TableLayoutDesc.newBuilder(
      KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED))
      .setLayoutId("full-featured-base")
      .build();
    mKiji.createTable(fullFeaturedLayout);

    // Build a new layout based on the first one, change values and call update table on it
    TableLayoutDesc updatedLayout = TableLayoutDesc.newBuilder(fullFeaturedLayout)
      .setLayoutId("full-featured-update")
      .setReferenceLayout(fullFeaturedLayout.getLayoutId())
      .setMaxFilesize(fullFeaturedLayout.getMaxFilesize() + 1000L)
      .setMemstoreFlushsize(fullFeaturedLayout.getMemstoreFlushsize() + 1000L)
      .setLocalityGroups(Lists.newArrayList(
        LocalityGroupDesc.newBuilder(
          fullFeaturedLayout.getLocalityGroups().get(0))
          .setBlockSize(1024)
          .setBloomType(BloomType.ROWCOL)
          .build(),
        fullFeaturedLayout.getLocalityGroups().get(1)))
      .build();
    mKiji.modifyTableLayout(updatedLayout);

    // Get the table descriptor
    HTableDescriptor hTableDescriptor = getHbaseTableDescriptor(FULL_FEATURED_TABLE_NAME);

    // Check the max file size value
    long actualMaxfilesize = hTableDescriptor.getMaxFileSize();
    assertEquals(EXPECTED_MAX_FILESIZE + 1000L, actualMaxfilesize);

    // Check the memstore flush size value
    long actualMemstoreFlushsize = hTableDescriptor.getMemStoreFlushSize();
    assertEquals(EXPECTED_MEMSTORE_FLUSHSIZE + 1000L, actualMemstoreFlushsize);

    ColumnNameTranslator columnNameTranslator =
      new ColumnNameTranslator(KijiTableLayout.newLayout(fullFeaturedLayout));
    KijiColumnName kijiColumnName = new KijiColumnName(FULL_FEATURED_TABLE_FAMILY_NAME,
      FULL_FEATURED_TABLE_QUALIFIER_NAME);
    HBaseColumnName columnName = columnNameTranslator.toHBaseColumnName(kijiColumnName);

    // Check the block size value
    HColumnDescriptor columnDescriptor = hTableDescriptor.getFamily(columnName.getFamily());
    int actualBlockSize = columnDescriptor.getBlocksize();
    assertEquals(1024, actualBlockSize);

    // Check the bloom type value
    StoreFile.BloomType actualBloomFilterType = columnDescriptor.getBloomFilterType();
    assertEquals(StoreFile.BloomType.ROWCOL, actualBloomFilterType);
  }

  /**
   * This tests the update of hbase attributes on a table layout that has none of the
   * layout-1.2 hbase attributes set (the base layout is a
   * programatically modified version of simple.json).
   */
  @Test
  public void testUpdateTableLayoutOnTableWithNoHBaseSettingsSet() throws IOException {
    // Create the table
    TableLayoutDesc simpleLayout = TableLayoutDesc.newBuilder(
      KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE))
      .setLayoutId("base")
      .setVersion("layout-1.2.0")
      .build();
    mKiji.createTable(simpleLayout);

    // Build a new layout based on the first one, change values and call update table on it
    long updatedMaxFilesize = 8 * 1024L * 1024L * 1024L;
    long updatedMemstoreFlushsize = 2 * 1024L * 1024L * 1024L;
    int updatedBlocksize = 2048;
    TableLayoutDesc updatedLayout = TableLayoutDesc.newBuilder(simpleLayout)
      .setLayoutId("updated")
      .setReferenceLayout(simpleLayout.getLayoutId())
      .setMaxFilesize(updatedMaxFilesize)
      .setMemstoreFlushsize(updatedMemstoreFlushsize)
      .setLocalityGroups(Lists.newArrayList(
        LocalityGroupDesc.newBuilder(
          simpleLayout.getLocalityGroups().get(0))
          .setBlockSize(updatedBlocksize)
          .setBloomType(BloomType.ROW)
          .build()))
      .build();
    mKiji.modifyTableLayout(updatedLayout);

    // Get the table descriptor
    HTableDescriptor hTableDescriptor = getHbaseTableDescriptor(SIMPLE_TABLE_NAME);

    // Check the max file size value
    long actualMaxfilesize = hTableDescriptor.getMaxFileSize();
    assertEquals(updatedMaxFilesize, actualMaxfilesize);

    // Check the memstore flush size value
    long actualMemstoreFlushsize = hTableDescriptor.getMemStoreFlushSize();
    assertEquals(updatedMemstoreFlushsize, actualMemstoreFlushsize);

    ColumnNameTranslator columnNameTranslator =
      new ColumnNameTranslator(KijiTableLayout.newLayout(simpleLayout));
    KijiColumnName kijiColumnName = new KijiColumnName(SIMPLE_TABLE_FAMILY_NAME,
      SIMPLE_TABLE_QUALIFIER_NAME);
    HBaseColumnName columnName = columnNameTranslator.toHBaseColumnName(kijiColumnName);

    // Check the block size value
    HColumnDescriptor columnDescriptor = hTableDescriptor.getFamily(columnName.getFamily());
    int actualBlockSize = columnDescriptor.getBlocksize();
    assertEquals(updatedBlocksize, actualBlockSize);

    // Check the bloom type value
    StoreFile.BloomType actualBloomFilterType = columnDescriptor.getBloomFilterType();
    assertEquals(StoreFile.BloomType.ROW, actualBloomFilterType);
  }

  private HTableDescriptor getHbaseTableDescriptor(String kijiTableName) throws IOException {
    KijiManagedHBaseTableName mPhysicalTableName =
      KijiManagedHBaseTableName.getKijiTableName(getKijiURI().getInstance(),
      kijiTableName);

    final HBaseFactory factory = HBaseFactory.Provider.get();
    Configuration mConf = HBaseConfiguration.create();
    HBaseAdmin mHBaseAdmin = factory.getHBaseAdminFactory(getHBaseURI()).create(mConf);
    byte[] tableNameAsBytes = Bytes.toBytes(mPhysicalTableName.toString());
    return mHBaseAdmin.getTableDescriptor(tableNameAsBytes);
  }
}
