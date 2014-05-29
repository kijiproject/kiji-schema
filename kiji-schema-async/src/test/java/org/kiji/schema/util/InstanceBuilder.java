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

package org.kiji.schema.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Used to build and populate a testing instance. This builder will construct a in-memory
 * Kiji cluster using an in-memory HBase implementation.
 *
 * Example usage:
 * <code><pre>
 * final Kiji instance = new InstanceBuilder()
 *     .withTable("table", layout)
 *         .withRow(id1)
 *             .withFamily("family")
 *                 .withQualifier("column").withValue(1L, "value1")
 *                                         .withValue(2L, "value2")
 *         .withRow(id2)
 *             .withFamily("family")
 *                 .withQualifier("column").withValue(100L, "value3")
 *     .build();
 * </pre></code>
 */
public class InstanceBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceBuilder.class);
  private static final AtomicLong FAKE_COUNT = new AtomicLong();

  /** Used to store cells to be built. */
  private final Map<String,
      Map<EntityId,
        Map<String,
          Map<String,
            Map<Long, Object>>>>> mCells;

  /** Used to store table layouts. */
  private final Map<String, KijiTableLayout> mLayouts;

  /** Name of the desired Kiji instance. */
  private final String mInstanceName;

  /** Pre-existing Kiji to use, or null. */
  private final Kiji mExistingKiji;

  /**
   * Constructs a new in-memory Kiji instance builder with the default Kiji instance name.
   */
  public InstanceBuilder() {
    this(UUID.randomUUID().toString().replaceAll("-", "_"));
  }

  /**
   * Constructs a new Kiji instance builder to populate a pre-existing Kiji instance.
   *
   * @param kiji Pre-existing (already installed) Kiji instance.
   */
  public InstanceBuilder(Kiji kiji) {
    mExistingKiji = Preconditions.checkNotNull(kiji);
    mInstanceName = kiji.getURI().getInstance();
    mCells = new HashMap<String, Map<EntityId, Map<String, Map<String, Map<Long, Object>>>>>();
    mLayouts = new HashMap<String, KijiTableLayout>();
  }

  /**
   * Constructs a new in-memory Kiji instance builder.
   *
   * @param instance Desired name of the Kiji instance.
   */
  public InstanceBuilder(String instance) {
    mExistingKiji = null;
    mInstanceName = instance;
    mCells = new HashMap<String, Map<EntityId, Map<String, Map<String, Map<Long, Object>>>>>();
    mLayouts = new HashMap<String, KijiTableLayout>();
  }

  /**
   * Adds a table to the testing environment.
   *
   * <p> Note: This will replace any existing added tables with the same name.
   *
   * @param table The name of the Kiji table to add.
   * @param layout The layout of the Kiji table being added.
   * @return A builder to continue building with.
   */
  public TableBuilder withTable(String table, KijiTableLayout layout) {
    mCells.put(table, new HashMap<EntityId, Map<String, Map<String, Map<Long, Object>>>>());
    mLayouts.put(table, layout);

    return new TableBuilder(table);
  }

  /**
   * Adds a table to the testing environment.
   *
   * <p> Note: This will replace any existing added tables with the same name.
   *
   * @param layoutDesc The layout descriptor of the Kiji table being added.
   * @return A builder to continue building with.
   * @throws InvalidLayoutException is the layout is invalid.
   */
  public TableBuilder withTable(TableLayoutDesc layoutDesc) throws InvalidLayoutException {
    final KijiTableLayout layout = KijiTableLayout.newLayout(layoutDesc);
    return withTable(layoutDesc.getName(), layout);
  }

  /**
   * Populate an existing table in the testing environment.
   *
   * @param table An existing Kiji table to populate.
   * @return A builder to continue building with.
   */
  public TableBuilder withTable(KijiTable table) {
    final KijiTableLayout layout = table.getLayout();
    mCells.put(layout.getName(),
        new HashMap<EntityId, Map<String, Map<String, Map<Long, Object>>>>());
    mLayouts.put(layout.getName(), layout);

    return new TableBuilder(layout.getName());
  }

  /**
   * Builds a test environment.
   *
   * <p> Creates an in-memory HBase cluster, populates and installs the provided Kiji instances.
   *
   * @return The kiji instance for the test environment.
   */
  public Kiji build() throws IOException {
    // Populate constants.
    final Configuration conf = HBaseConfiguration.create();
    final KijiURI uri = (mExistingKiji != null)
        ? mExistingKiji.getURI()
        : KijiURI.newBuilder(
            String.format("kiji://.fake.%d/%s", FAKE_COUNT.getAndIncrement(), mInstanceName))
            .build();

    // In-process MapReduce execution:
    // TODO(KIJIMR-19): remove this, InstanceBuilder should not be concerned by configuration.
    //     This is a temporary fix until all job builders have a withConf() setter.
    final String tmpDir = "file:///tmp/hdfs-testing-" + System.nanoTime();
    conf.set("fs.default.FS", tmpDir);
    conf.set("mapred.job.tracker", "local");

    // Install & open a Kiji instance.
    LOG.info(String.format("Building instance: %s", uri.toString()));
    try {
      if (mExistingKiji == null) {
        KijiInstaller.get().install(uri, conf);
      }
    } catch (KijiInvalidNameException kine) {
      throw new IOException(kine);
    }
    final Kiji kiji = (mExistingKiji != null)
        ? mExistingKiji
        : Kiji.Factory.open(uri, conf);

    // Build tables.
    for (Map.Entry<String, Map<EntityId, Map<String, Map<String, Map<Long, Object>>>>> tableEntry
        : mCells.entrySet()) {
      final String tableName = tableEntry.getKey();
      final KijiTableLayout layout = mLayouts.get(tableName);
      final Map<EntityId, Map<String, Map<String, Map<Long, Object>>>> table =
          tableEntry.getValue();

      // Create & open a Kiji table.
      if (kiji.getTableNames().contains(tableName)) {
        LOG.info(String.format("  Populating existing table: %s", tableName));
      } else {
        LOG.info(String.format("  Creating and populating table: %s", tableName));
        kiji.createTable(layout.getDesc());
      }
      final KijiTable kijiTable = kiji.openTable(tableName);
      try {
        final KijiTableWriter writer = kijiTable.openTableWriter();
        try {
          // Build & write rows to the table.
          for (Map.Entry<EntityId, Map<String, Map<String, Map<Long, Object>>>> rowEntry
              : table.entrySet()) {
            final EntityId entityId = rowEntry.getKey();
            final Map<String, Map<String, Map<Long, Object>>> row = rowEntry.getValue();
            for (Map.Entry<String, Map<String, Map<Long, Object>>> familyEntry : row.entrySet()) {
              final String familyName = familyEntry.getKey();
              final Map<String, Map<Long, Object>> family = familyEntry.getValue();
              for (Map.Entry<String, Map<Long, Object>> qualifierEntry : family.entrySet()) {
                final String qualifierName = qualifierEntry.getKey();
                final Map<Long, Object> qualifier = qualifierEntry.getValue();
                for (Map.Entry<Long, Object> valueEntry : qualifier.entrySet()) {
                  final long timestamp = valueEntry.getKey();
                  final Object value = valueEntry.getValue();
                  LOG.info("\tBuilding put: {} -> ({}:{}, {}:{})",
                      entityId, familyName, qualifierName, timestamp, value);
                  writer.put(entityId, familyName, qualifierName, timestamp, value);
                }
              }
            }
          }
        } finally {
          writer.close();
        }
      } finally {
        kijiTable.release();
      }
    }

    // Add the Kiji instance to the environment.
    return kiji;
  }

  /**
   * A builder used to build and populate an in-memory Kiji table.
   */
  public class TableBuilder {
    protected final String mTableName;
    private final EntityIdFactory mEntityIdFactory;

    /**
     * Constructs a new in-memory Kiji table builder.
     *
     * @param table Desired name of the Kiji table.
     */
    protected TableBuilder(String table) {
      final KijiTableLayout layout = mLayouts.get(table);

      mTableName = table;
      mEntityIdFactory = EntityIdFactory.getFactory(layout);
    }

    /**
     * Adds a table to the testing environment. Note: This will replace any existing added tables
     * with the same name.
     *
     * @param table The name of the Kiji table to add.
     * @param layout The layout of the Kiji table being added.
     * @return A builder to continue building with.
     */
    public TableBuilder withTable(String table, KijiTableLayout layout) {
      mCells.put(table, new HashMap<EntityId, Map<String, Map<String, Map<Long, Object>>>>());
      mLayouts.put(table, layout);

      return new TableBuilder(table);
    }

    /**
     * Adds a table to the testing environment.
     *
     * <p> Note: This will replace any existing added tables with the same name.
     *
     * @param layoutDesc The layout descriptor of the Kiji table being added.
     * @return A builder to continue building with.
     * @throws InvalidLayoutException is the layout is invalid.
     */
    public TableBuilder withTable(TableLayoutDesc layoutDesc) throws InvalidLayoutException {
      final KijiTableLayout layout = KijiTableLayout.newLayout(layoutDesc);
      return withTable(layoutDesc.getName(), layout);
    }

    /**
     * Adds a row to the testing environment. Note: This will replace any existing added rows
     * with the same entityId.
     *
     * @param entityId The entityId of the row being added.
     * @return A builder to continue building with.
     */
    public RowBuilder withRow(EntityId entityId) {
      mCells
          .get(mTableName)
          .put(entityId, new HashMap<String, Map<String, Map<Long, Object>>>());

      return new RowBuilder(mTableName, entityId);
    }

    /**
     * Adds a row to the testing environment. Note: This will replace any existing added rows
     * with the same entityId.
     *
     * @param components Components of the entity ID for the row to build.
     * @return A builder to continue building with.
     */
    public RowBuilder withRow(Object... components) {
      return withRow(mEntityIdFactory.getEntityId(components));
    }

    /**
     * Builds a test environment.
     *
     * @return The kiji instances for the test environment.
     */
    public Kiji build() throws IOException {
      return InstanceBuilder.this.build();
    }
  }

  /**
   * A builder used to build and populate an in-memory Kiji row.
   */
  public class RowBuilder extends TableBuilder {
    protected final EntityId mEntityId;

    /**
     * Constructs a new in-memory Kiji row builder.
     *
     * @param table Name of the Kiji table that this row will belong to.
     * @param entityId Desired entityId of the row.
     */
    protected RowBuilder(String table, EntityId entityId) {
      super(table);
      mEntityId = entityId;
    }

    /**
     * Adds a column family to the testing environment. Note: This will replace any existing added
     * column families with the same name.
     *
     * @param family Name of the column family being added.
     * @return A builder to continue building with.
     */
    public FamilyBuilder withFamily(String family) {
      mCells
          .get(mTableName)
          .get(mEntityId)
          .put(family, new HashMap<String, Map<Long, Object>>());

      return new FamilyBuilder(mTableName, mEntityId, family);
    }
  }

  /**
   * A builder used to build and populate an in-memory column family.
   */
  public class FamilyBuilder extends RowBuilder {
    protected final String mFamilyName;

    /**
     * Constructs a new in-memory Kiji column family builder.
     *
     * @param table Name of the Kiji table that this column family will belong to.
     * @param entityId EntityId of the row that this column family will belong to.
     * @param family Desired column family name.
     * @return A builder to continue building with.
     */
    protected FamilyBuilder(String table, EntityId entityId, String family) {
      super(table, entityId);
      mFamilyName = family;
    }

    /**
     * Adds a qualified column to the testing environment. Note: This will replace any existing
     * added qualified column with the same name.
     *
     * @param qualifier Name of the qualified column being added.
     * @return A builder to continue building with.
     */
    public QualifierBuilder withQualifier(String qualifier) {
      mCells
          .get(mTableName)
          .get(mEntityId)
          .get(mFamilyName)
          .put(qualifier, new TreeMap<Long, Object>());

      return new QualifierBuilder(mTableName, mEntityId, mFamilyName, qualifier);
    }
  }

  /**
   * A builder used to build an populate an in-memory Kiji cell.
   */
  public class QualifierBuilder extends FamilyBuilder {
    protected final String mQualifierName;

    /**
     * Constructs a new in-memory Kiji cell builder.
     *
     * @param table Name of the Kiji table that this column will belong to.
     * @param entityId EntityId of the row that this column will belong to.
     * @param family Name of the column family that this column will belong to.
     * @param qualifier Desired column name.
     * @return A builder to continue building with.
     */
    protected QualifierBuilder(String table, EntityId entityId, String family, String qualifier) {
      super(table, entityId, family);
      mQualifierName = qualifier;
    }

    /**
     * Adds a timestamped value to the testing environment. Note: This will write a value with
     * the current time as its timestamp.
     *
     * @param value The value.
     * @return A builder to continue building with.
     */
    public QualifierBuilder withValue(Object value) {
      return withValue(HConstants.LATEST_TIMESTAMP, value);
    }

    /**
     * Adds a timestamped value to the testing environment. Note: This will replace existing
     * values with the same timestamp.
     *
     * @param timestamp Timestamp for the data.
     * @param value The value.
     * @return A builder to continue building with.
     */
    public QualifierBuilder withValue(long timestamp, Object value) {
      mCells
          .get(mTableName)
          .get(mEntityId)
          .get(mFamilyName)
          .get(mQualifierName)
          .put(timestamp, value);

      return this;
    }
  }
}
