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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAdmin;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.impl.DefaultHBaseFactory;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Used to build and populate a testing environment. This builder will construct a in-memory
 * Kiji cluster using an in-memory HBase implementation.
 *
 * Example usage:
 * <code><pre>
 * final Map<String, Kiji> environment = new EnvironmentBuilder()
 *     .withInstance("inst1")
 *         .withTable("table", layout)
 *             .withRow(id1)
 *                 .withFamily("family")
 *                     .withQualifier("column").withValue(1L, "value1")
 *                                             .withValue(2L, "value2")
 *             .withRow(id2)
 *                 .withFamily("family")
 *                     .withQualifier("column").withValue(100L, "value3")
 *     .build();
 * </pre></code>
 */
public class EnvironmentBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceBuilder.class);

  /** Used to store cells to be built. */
  private final Map<String,
      Map<String,
        Map<EntityId,
          Map<String,
            Map<String,
              Map<Long, Object>>>>>> mCells;

  /** Used to store table layouts. */
  private final Map<String, Map<String, KijiTableLayout>> mLayouts;

  /**
   * Creates a new empty testing environment builder.
   */
  public EnvironmentBuilder() {
    mCells = new HashMap<String, Map<String, Map<EntityId, Map<String, Map<String,
        Map<Long, Object>>>>>>();
    mLayouts = new HashMap<String, Map<String, KijiTableLayout>>();
  }

  /**
   * Adds an instance to the testing environment. Note: This will replace any existing added
   * instances with the same name.
   *
   * @param instance The name of the Kiji instance to add.
   * @return A builder to continue building with.
   */
  public InstanceBuilder withInstance(String instance) {
    mCells.put(instance,
        new HashMap<String, Map<EntityId, Map<String, Map<String, Map<Long, Object>>>>>());
    mLayouts.put(instance,
        new HashMap<String, KijiTableLayout>());

    return new InstanceBuilder(instance);
  }

  /**
   * Builds a test environment. Creates an in-memory HBase cluster, populates and installs
   * the provided Kiji instances.
   *
   * @return The kiji instances for the test environment.
   */
  public Map<String, Kiji> build() throws IOException {
    LOG.info("Building environment...");
    final Map<String, Kiji> environment = new HashMap<String, Kiji>();
    for (Map.Entry<String, Map<String, Map<EntityId, Map<String, Map<String, Map<Long, Object>>>>>>
        instanceEntry : mCells.entrySet()) {
      // Populate constants.
      final String instanceName = instanceEntry.getKey();
      final Map<String, Map<EntityId, Map<String, Map<String, Map<Long, Object>>>>> instance =
          instanceEntry.getValue();
      final int id = environment.size();
      final Configuration conf = HBaseConfiguration.create();

      // Build fake Kiji URI.
      KijiURI uri;
      try {
        uri = KijiURI.parse(
            String.format("kiji://.fake.%d/%s", id, instanceName));
      } catch (KijiURIException kue) {
        throw new IOException(kue);
      }
      LOG.info(String.format("  Building instance: %s", uri.toString()));

      // In-process Map/Reduce execution:
      conf.set("mapred.job.tracker", "local");

      // Install & open a Kiji instance.
      try {
        KijiInstaller.install(uri, conf);
      } catch (KijiInvalidNameException kine) {
        throw new IOException(kine);
      }
      final Kiji kiji = Kiji.Factory.open(uri, conf);

      // Build tables.
      final KijiAdmin admin =
          new KijiAdmin(
              DefaultHBaseFactory.Provider.get().getHBaseAdminFactory(uri).create(conf),
              kiji);
      for (Map.Entry<String, Map<EntityId, Map<String, Map<String, Map<Long, Object>>>>> tableEntry
          : instance.entrySet()) {
        final String tableName = tableEntry.getKey();
        final KijiTableLayout layout = mLayouts.get(instanceName).get(tableName);
        final Map<EntityId, Map<String, Map<String, Map<Long, Object>>>> table =
            tableEntry.getValue();

        // Create & open a Kiji table.
        LOG.info(String.format("    Building table: %s", tableName));
        admin.createTable(tableName, layout, false);
        final KijiTable kijiTable = kiji.openTable(tableName);
        final KijiTableWriter writer = kijiTable.openTableWriter();

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
                LOG.info(String.format("      Building put: %s -> (%s:%s, %d:%s)",
                    entityId.toString(),
                    familyName,
                    qualifierName,
                    timestamp,
                    value.toString()));
                writer.put(
                    entityId,
                    familyName,
                    qualifierName,
                    timestamp,
                    value);
              }
            }
          }
        }

        IOUtils.closeQuietly(kijiTable);
        IOUtils.closeQuietly(writer);
      }

      // Add the Kiji instance to the environment.
      environment.put(instanceName, kiji);
    }

    return environment;
  }

  /**
   * A builder used to build and populate an in-memory Kiji instance.
   */
  public class InstanceBuilder {
    protected final String mInstanceName;

    /**
     * Constructs a new in-memory Kiji instance builder.
     *
     * @param instance Desired name of the Kiji instance.
     */
    protected InstanceBuilder(String instance) {
      mInstanceName = instance;
    }

    /**
     * Adds an instance to the testing environment. Note: This will replace any existing added
     * instances with the same name.
     *
     * @param instance The name of the Kiji instance to add.
     * @return A builder to continue building with.
     */
    public InstanceBuilder withInstance(String instance) {
      mCells.put(instance,
          new HashMap<String, Map<EntityId, Map<String, Map<String, Map<Long, Object>>>>>());
      mLayouts.put(instance,
          new HashMap<String, KijiTableLayout>());

      return new InstanceBuilder(instance);
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
      mCells
          .get(mInstanceName)
          .put(table, new HashMap<EntityId, Map<String, Map<String, Map<Long, Object>>>>());
      mLayouts
        .get(mInstanceName)
        .put(table, layout);

      return new TableBuilder(mInstanceName, table);
    }

    /**
     * Builds a test environment.
     *
     * @return The kiji instances for the test environment.
     */
    public Map<String, Kiji> build() throws IOException {
      return EnvironmentBuilder.this.build();
    }
  }

  /**
   * A builder used to build and populate an in-memory Kiji table.
   */
  public class TableBuilder extends InstanceBuilder {
    protected final String mTableName;
    private final EntityIdFactory mEntityIdFactory;

    /**
     * Constructs a new in-memory Kiji table builder.
     *
     * @param instance Name of the Kiji instance that this table will belong to.
     * @param table Desired name of the Kiji table.
     */
    protected TableBuilder(String instance, String table) {
      super(instance);
      final KijiTableLayout layout = mLayouts.get(instance).get(table);

      mTableName = table;
      mEntityIdFactory = EntityIdFactory.create(layout.getDesc().getKeysFormat());
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
          .get(mInstanceName)
          .get(mTableName)
          .put(entityId, new HashMap<String, Map<String, Map<Long, Object>>>());

      return new RowBuilder(mInstanceName, mTableName, entityId);
    }

    /**
     * Adds a row to the testing environment. Note: This will replace any existing added rows
     * with the same entityId.
     *
     * @param rowKey The key of the row being added.
     * @return A builder to continue building with.
     */
    public RowBuilder withRow(String rowKey) {
      return withRow(mEntityIdFactory.fromKijiRowKey(rowKey));
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
     * @param instance Name of the Kiji instance that this row will belong to.
     * @param table Name of the Kiji table that this row will belong to.
     * @param entityId Desired entityId of the row.
     * @return A builder to continue building with.
     */
    protected RowBuilder(String instance, String table, EntityId entityId) {
      super(instance, table);
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
          .get(mInstanceName)
          .get(mTableName)
          .get(mEntityId)
          .put(family, new HashMap<String, Map<Long, Object>>());

      return new FamilyBuilder(mInstanceName, mTableName, mEntityId, family);
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
     * @param instance Name of the Kiji instance that this column family will belong to.
     * @param table Name of the Kiji table that this column family will belong to.
     * @param entityId EntityId of the row that this column family will belong to.
     * @param family Desired column family name.
     * @return A builder to continue building with.
     */
    protected FamilyBuilder(String instance, String table, EntityId entityId, String family) {
      super(instance, table, entityId);
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
          .get(mInstanceName)
          .get(mTableName)
          .get(mEntityId)
          .get(mFamilyName)
          .put(qualifier, new TreeMap<Long, Object>());

      return new QualifierBuilder(mInstanceName, mTableName, mEntityId, mFamilyName, qualifier);
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
     * @param instance Name of the Kiji instance that this column will belong to.
     * @param table Name of the Kiji table that this column will belong to.
     * @param entityId EntityId of the row that this column will belong to.
     * @param family Name of the column family that this column will belong to.
     * @param qualifier Desired column name.
     * @return A builder to continue building with.
     */
    protected QualifierBuilder(String instance, String table, EntityId entityId, String family,
        String qualifier) {
      super(instance, table, entityId, family);
      mQualifierName = qualifier;
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
          .get(mInstanceName)
          .get(mTableName)
          .get(mEntityId)
          .get(mFamilyName)
          .get(mQualifierName)
          .put(timestamp, value);

      return this;
    }
  }
}
