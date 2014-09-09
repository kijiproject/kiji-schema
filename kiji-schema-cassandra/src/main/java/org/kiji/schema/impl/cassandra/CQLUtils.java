/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.impl.cassandra;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KConstants;
import org.kiji.schema.KijiRowKeyComponents;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Provides utitity methods and constants for constructing CQL statements.
 *
 * Notes on Kiji & Cassandra data model Entity ID to Primary Key translation
 *
 * Cassandra (CQL) has the notion of a primary key, which consists of 1 or more CQL columns.  A
 * primary key composed of >1 column is a compound primary key.  For example, the following table
 * definition has a compound primary key consisting of two columns (c1, c2):
 *
 *    CREATE TABLE t1 (
 *      c1 varchar,
 *      c2 int,
 *      c3 blob,
 *      PRIMARY KEY (c1, c2)
 *    )
 *
 * The first element of a compound primary key (or the sole element of a non-compound primary key)
 * is the partition key. For example, in table t1, c1 is the partition key. The partition key is
 * tokenized in order to determine what partition a row will fall into. IUD operations on rows
 * with the same partition key are performed atomically and in isolation (theoretically).
 *
 * The remaining elements of a primary key (if they exist) are referred to as the clustering
 * columns.  For example, in table t1, c2 is the sole clustering column.
 *
 * Partition keys can be made up of multiple columns using a composite partition key, for example:
 *
 *    CREATE TABLE t2 (
 *      c1 uuid,
 *      c2 varchar,
 *      c3 int,
 *      c4 int,
 *      c5 blob,
 *      PRIMARY KEY((c1, c2), c3, c4)
 *    );
 *
 * Table t2 has a composite partition key consisting of c1 and c2. Table t2 has clustering columns
 * c3 and c4.
 *
 * Kiji RowKeyFormat2 defines 2 valid entity ID formats: formatted and raw.
 *
 * Formatted: formatted entity IDs consist of 1 or more components of type STRING, INT, or LONG.
 *            additionally, 1 or more of the components (in sequence) must be hashed.  The hashed
 *            components correspond exactly to the partition key of the CQL primary key.  The
 *            unhashed components correspond exactly to the clustering columns of the CQL primary
 *            key. The name of the columns will match the component names of the entity ID.
 *
 * Raw: raw entity IDs consist of a single byte array blob component. This single component
 *      corresponds to the partition key of the CQL primary key. There are no clustering columns
 *      in the CQL primary key. The name of the single primary key column will be a constant.
 */
public final class CQLUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CQLUtils.class);

  // Useful static members for referring to different fields in the C* tables.
  public static final String RAW_KEY_COL = "key";
  public static final String LOCALITY_GROUP_COL = "lg";
  public static final String FAMILY_COL = "family";
  public static final String QUALIFIER_COL = "qualifier";
  public static final String VERSION_COL = "version";
  public static final String VALUE_COL = "value";

  private static final String BYTES_TYPE = "blob";
  private static final String STRING_TYPE = "varchar";
  private static final String INT_TYPE = "int";
  private static final String LONG_TYPE = "bigint";
  private static final String COUNTER_TYPE = "counter";

  private static final Joiner COMMA_JOINER = Joiner.on(", ");

  /**
   * Private constructor for utility class.
   */
  private CQLUtils() {
  }

  /**
   * Return the columns and their associated types of the primary key for the associated table
   * layout. The returned LinkedHashMap can be iterated through in primary key column order.
   *
   * @param layout to get primary key column and types for.
   * @return a map of column name to CQL column type with proper iteration order.
   */
  private static LinkedHashMap<String, String> getPrimaryKeyColumnTypes(KijiTableLayout layout) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    LinkedHashMap<String, String> map = Maps.newLinkedHashMap();
    switch (keyFormat.getEncoding()) {
      case RAW: {
        map.put(RAW_KEY_COL, BYTES_TYPE);
        break;
      }
      case FORMATTED: {
        for (RowKeyComponent component : keyFormat.getComponents()) {
          map.put(
              translateEntityIDComponentNameToColumnName(component.getName()),
              getCQLType(component.getType()));
        }
        break;
      }
      default: throw new IllegalArgumentException(
          String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
    map.put(LOCALITY_GROUP_COL, STRING_TYPE);
    map.put(FAMILY_COL, BYTES_TYPE);
    map.put(QUALIFIER_COL, BYTES_TYPE);
    map.put(VERSION_COL, LONG_TYPE);
    return map;
  }

  /**
   * Translates an EntityID ComponentType into a CQL type.
   *
   * @param type of entity id component to get CQL type for.
   * @return the CQL type of the provided ComponentType.
   */
  private static String getCQLType(ComponentType type) {
    switch (type) {
      case INTEGER: return INT_TYPE;
      case LONG: return LONG_TYPE;
      case STRING: return STRING_TYPE;
      default: throw new IllegalArgumentException();
    }
  }

  /**
   * Retrieves the entity ID component values from an entity ID, and does any necessary
   * transformations on the values to make them CQL compliant.
   *
   * Currently the only conversions that needs to happen is byte[] -> ByteBuffer for RAW formatted
   * entity IDs.
   *
   * @param layout of table.
   * @param entityID containing components.
   * @return a list of entityID components.
   */
  private static List<Object> getEntityIDComponentValues(
      KijiTableLayout layout,
      EntityId entityID) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: return ImmutableList.of(
          (Object) CassandraByteUtil.bytesToByteBuffer((byte[]) entityID.getComponentByIndex(0)));
      case FORMATTED: return entityID.getComponents();
      default: throw new IllegalArgumentException(
          String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }

  /**
   * Return the ordered list of columns in the primary key for the table layout.
   *
   * @param layout to return primary key columns for.
   * @return the primary key columns for the layout.
   */
  private static List<String> getPrimaryKeyColumns(KijiTableLayout layout) {
    List<String> columns = getEntityIDColumns(layout);
    columns.add(LOCALITY_GROUP_COL);
    columns.add(FAMILY_COL);
    columns.add(QUALIFIER_COL);
    columns.add(VERSION_COL);
    return columns;
  }

  /**
   * Return the ordered list of columns in the Kiji entity ID for the table layout.
   *
   * @param layout to return entity ID columns for
   * @return the entity ID columns of the table.
   */
  public static List<String> getEntityIDColumns(KijiTableLayout layout) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: return Lists.newArrayList(RAW_KEY_COL);
      case FORMATTED: return transformToColumns(keyFormat.getComponents());
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }

  /**
   * Return the ordered list of columns in the partition key for the table layout.
   *
   * @param layout to return partition key columns for.
   * @return the primary key columns for the layout.
   */
  public static List<String> getPartitionKeyColumns(KijiTableLayout layout) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: return Lists.newArrayList(RAW_KEY_COL);
      case FORMATTED:
        return transformToColumns(
            keyFormat.getComponents().subList(0, keyFormat.getRangeScanStartIndex()));
      default:
        throw new IllegalArgumentException(
          String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }

  /**
   * Return the ordered list of cluster columns for the table layout.
   *
   * @param layout to return cluster columns for.
   * @return the primary key columns for the layout.
   */
  public static List<String> getClusterColumns(KijiTableLayout layout) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    List<String> columns;
    switch (keyFormat.getEncoding()) {
      case RAW: {
        columns = Lists.newArrayList();
        break;
      }
      case FORMATTED: {
        int size = keyFormat.getComponents().size();
        int start = keyFormat.getRangeScanStartIndex();
        if (start == size) {
          columns = Lists.newArrayList();
        } else {
          columns = transformToColumns(
              keyFormat
                  .getComponents()
                  .subList(keyFormat.getRangeScanStartIndex(), keyFormat.getComponents().size()));
        }
        break;
      }
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
    columns.add(LOCALITY_GROUP_COL);
    columns.add(FAMILY_COL);
    columns.add(QUALIFIER_COL);
    columns.add(VERSION_COL);
    return columns;
  }

  /**
   * Return the CQL token column for a Kiji table layout.
   *
   * @param layout to create CQL token column for.
   * @return the CQL token column for the layout.
   */
  public static String getTokenColumn(KijiTableLayout layout) {
    return String.format("token(%s)", COMMA_JOINER.join(getPartitionKeyColumns(layout)));
  }

  /**
   * Given the name of an entity ID component, returns the corresponding Cassandra column name.
   *
   * Inserts a prefix to make sure that the column names for entity ID components don't conflict
   * with CQL reserved words or with other column names in Kiji Cassandra tables.
   *
   * @param entityIDComponentName The name of the entity ID component.
   * @return the name of the Cassandra column for this component.
   */
  public static String translateEntityIDComponentNameToColumnName(
      final String entityIDComponentName
  ) {
    return "eid_" + entityIDComponentName;
  }

  /**
   * Extract the Entity ID components from a row for a given table layout.
   *
   * @param layout of the table.
   * @param row to extract Entity ID components from.
   * @return the Entity ID components of the row.
   */
  public static KijiRowKeyComponents getRowKeyComponents(KijiTableLayout layout, Row row) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    Object[] components;

    switch (keyFormat.getEncoding()) {
      case RAW: {
        components = new Object[] {CassandraByteUtil.byteBuffertoBytes(row.getBytes(RAW_KEY_COL))};
        break;
      }
      case FORMATTED: {
        List<RowKeyComponent> formatComponents = keyFormat.getComponents();
        components = new Object[formatComponents.size()];

        for (int i = 0; i < formatComponents.size(); i++) {
          RowKeyComponent component = formatComponents.get(i);
          final String columnName = translateEntityIDComponentNameToColumnName(component.getName());
          switch (component.getType()) {
            case STRING: {
              components[i] = row.getString(columnName);
              break;
            }
            case INTEGER: {
              components[i] = row.getInt(columnName);
              break;
            }
            case LONG: {
              components[i] = row.getLong(columnName);
              break;
            }
            default: throw new IllegalArgumentException("Unknown row key component type.");
          }
        }
        break;
      }
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
    return KijiRowKeyComponents.fromComponents(components);
  }

  /**
   * Transforms a list of RowKeyComponents into a list of the column names.
   *
   * @param components to transform into columns.
   * @return a list of columns.
   */
  private static List<String> transformToColumns(List<RowKeyComponent> components) {
    List<String> list = Lists.newArrayList();
    for (RowKeyComponent component : components) {
      list.add(translateEntityIDComponentNameToColumnName(component.getName()));
    }
    return list;
  }

  /**
   * Returns a 'CREATE TABLE' statement for the provided table name and table layout.
   *
   * @param tableName of table to be created.
   * @param layout of kiji table.
   * @return a CQL 'CREATE TABLE' statement which will create the provided table.
   */
  public static String getCreateTableStatement(
      CassandraTableName tableName,
      KijiTableLayout layout) {
    return getCreateTableStatement(tableName, layout, BYTES_TYPE);
  }

  /**
   * Returns a 'CREATE TABLE' statement for the counter table for the provided table name and table
   * layout.
   *
   * @param tableName of table to be created.
   * @param layout of kiji table.
   * @return a CQL 'CREATE TABLE' statement which will create the provided table's counter table.
   */
  public static String getCreateCounterTableStatement(
      CassandraTableName tableName,
      KijiTableLayout layout) {
    return getCreateTableStatement(tableName, layout, COUNTER_TYPE);
  }

  /**
   * Creates a 'CREATE TABLE' statement for the provided table and layout with the provided value
   * column type.
   *
   * @param tableName of table to be created.
   * @param layout of kiji table.
   * @param valueType type to assign to the value column.
   * @return a CQL 'CREATE TABLE' statement for the table.
   */
  private static String getCreateTableStatement(
      CassandraTableName tableName,
      KijiTableLayout layout,
      String valueType) {
    LinkedHashMap<String, String> columns = getPrimaryKeyColumnTypes(layout);
    columns.put(VALUE_COL, valueType);

    // statement being built:
    //  "CREATE TABLE ${tableName} (
    //   ${PKColumn1} ${PKColumn1Type}, ${PKColumn2} ${PKColumn2Type}..., ${VALUE_COL} ${valueType}
    //   PRIMARY KEY ((${PartitionKeyComponent1} ${type}, ${PartitionKeyComponent2} ${type}...),
    //                ${ClusterColumn1} ${type}, ${ClusterColumn2} ${type}..))
    //   WITH CLUSTERING
    //   ORDER BY (${ClusterColumn1} ASC, ${ClusterColumn2} ASC..., ${VERSION_COL} DESC);

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(tableName).append(" (");

    COMMA_JOINER.withKeyValueSeparator(" ").appendTo(sb, columns);

    sb.append(", PRIMARY KEY ((");
    COMMA_JOINER.appendTo(sb, getPartitionKeyColumns(layout));
    sb.append(")");

    List<String> clusterColumns = getClusterColumns(layout);
    if (clusterColumns.size() > 0) {
      sb.append(", ");
    }
    COMMA_JOINER.appendTo(sb, clusterColumns);

    sb.append(")) WITH CLUSTERING ORDER BY (");
    Joiner.on(" ASC, ").appendTo(sb, clusterColumns);

    sb.append(" DESC);");

    String query = sb.toString();

    LOG.info("Prepared query string for table create: {}", query);

    return query;
  }

  /**
   * Returns a CQL statement which will create a secondary index on the provided table and column.
   *
   * @param tableName of table to create index on.
   * @param columnName of column to create index on.
   * @return a CQL statement to create a secondary index on the provided table and column.
   */
  public static String getCreateIndexStatement(CassandraTableName tableName, String columnName) {
    return String.format("CREATE INDEX ON %s (%s)", tableName, columnName);
  }

  // CSOFF: ParameterNumberCheck
  /**
   * Create a CQL statement for selecting a column from a row of a Cassandra Kiji table.
   *
   * @param admin Cassandra context object.
   * @param layout table layout of table.
   * @param tableName translated table name as known by Cassandra.
   * @param entityId of row to select.
   * @param column to get.  May be unqualified.
   * @param version being requested, or null if no version. May not be used with minVersion or
   *                maxVersion.
   * @param minVersion being requested (inclusive), or null if no minimum version. May not be used
   *                   with version.
   * @param maxVersion being requested (exclusive), or null if no maximum version. May not be used
   *                   with version.
   * @param numVersions being requested, or null for all version.
   * @return a statement which will get the column.
   */
  public static Statement getColumnGetStatement(
      CassandraAdmin admin,
      KijiTableLayout layout,
      CassandraTableName tableName,
      EntityId entityId,
      CassandraColumnName column,
      Long version,
      Long minVersion,
      Long maxVersion,
      Integer numVersions
  ) {
    // Normalize min/max version
    maxVersion = maxVersion == null || maxVersion == Long.MAX_VALUE ? null : maxVersion;
    minVersion = minVersion == null || minVersion <= 0L ? null : minVersion;

    Preconditions.checkArgument(
        (version == null && minVersion == null && maxVersion == null) || column.containsQualifier(),
        "Deletes with version must be qualified.");
    Preconditions.checkArgument(!(version != null && (minVersion != null || maxVersion != null)),
        "Cannot specify version and minVersion or maxVersion.");
    Preconditions.checkArgument(column.containsFamily(),
        "Column must contain a locality group and family.");

    // statement being built:
    //  "SELECT * FROM ${tableName} WHERE ${EIDColumn1}=? AND ${EIDColumn2}=? ...
    //   AND ${LOCALITY_GROUP_COL}=? AND ${FAMILY_COL}=? [AND ${QUALIFIER_COL}=?]
    //  [AND ${VERSION_COL} = ?] [AND ${VERSION_COL}>=?] [AND ${VERSION_COL}<?]
    //  [LIMIT ${column.getMaxVersions()]}

    List<String> columns = getEntityIDColumns(layout);
    columns.add(LOCALITY_GROUP_COL);
    columns.add(FAMILY_COL);
    if (column.containsQualifier()) {
      columns.add(QUALIFIER_COL);
    }
    if (version != null) {
      columns.add(VERSION_COL);
    }

    StringBuilder sb = new StringBuilder()
        .append("SELECT * FROM ")
        .append(tableName)
        .append(" WHERE ");

    Joiner.on("=? AND ").appendTo(sb, columns);
    sb.append("=?");

    if (minVersion != null) {
      sb.append(" AND ").append(VERSION_COL).append(">=?");
    }
    if (maxVersion != null) {
      sb.append(" AND ").append(VERSION_COL).append("<?");
    }
    if (numVersions != null) {
      sb.append(" LIMIT ?");
    }

    String query = sb.toString();

    LOG.debug("Prepared query string for column get: {}", query);

    List<Object> bindValues = Lists.newArrayList();
    bindValues.addAll(getEntityIDComponentValues(layout, entityId));
    bindValues.add(column.getLocalityGroup());
    bindValues.add(column.getFamilyBuffer());
    if (column.containsQualifier()) {
      bindValues.add(column.getQualifierBuffer());
    }
    if (version != null) {
      bindValues.add(version);
    }
    if (minVersion != null) {
      bindValues.add(minVersion);
    }
    if (maxVersion != null) {
      bindValues.add(maxVersion);
    }
    if (numVersions != null) {
      bindValues.add(numVersions);
    }

    return admin.getPreparedStatement(query).bind(bindValues.toArray());
  }
  // CSOFF

  /**
   * Create a CQL statement for selecting a single row from a column of a Cassandra Kiji table.
   *
   * @param admin Cassandra context object.
   * @param layout table layout of table.
   * @param tableName translated table name as known by Cassandra.
   * @param scannerOptions Cassandra specific scanner options (e.g., start and end tokens).
   * @return a statement which will get the column.
   */
  public static Statement getColumnScanStatement(
      CassandraAdmin admin,
      KijiTableLayout layout,
      CassandraTableName tableName,
      CassandraKijiScannerOptions scannerOptions) {
    Preconditions.checkNotNull(scannerOptions);

    // statement being built:
    //  "SELECT token(${PartitionKeyComponent1}, ${PartitionKeyComponent2} ...),
    //          ${PKColumn1}, ${PKColumn2}..., ${VALUE_COL}
    //   FROM ${tableName}

    // Note that this query does not use any WHERE clauses and it does not enable ALLOW FILTERING.
    // This means that we will fetch *all* of that data for each row for the Cassandra table in
    // question.  We do so because using WHERE clauses and ALLOW FILTERING caused major performance
    // problems.

    List<String> tokenColumns = getPartitionKeyColumns(layout);
    List<String> fromColumns = getPrimaryKeyColumns(layout);
    fromColumns.add(VALUE_COL);

    StringBuilder sb = new StringBuilder();

    sb.append("SELECT token(");
    COMMA_JOINER.appendTo(sb, tokenColumns);
    sb.append("), ");
    COMMA_JOINER.appendTo(sb, fromColumns);
    sb.append(" FROM ")
      .append(tableName);

    if (scannerOptions.hasStartToken() || scannerOptions.hasStopToken()) {
      sb.append(" WHERE ");
    }

    if (scannerOptions.hasStartToken()) {
      sb.append(" token(");
      COMMA_JOINER.appendTo(sb, tokenColumns);
      sb.append(") >= ?");
    }

    if (scannerOptions.hasStopToken()) {
      if (scannerOptions.hasStartToken()) {
        sb.append(" AND ");
      }
      sb.append(" token(");
      COMMA_JOINER.appendTo(sb, tokenColumns);
      sb.append(") <= ?");
    }

    String query = sb.toString();

    LOG.debug("Prepared query string for single column scan: {}", query);

    List<Object> bindValues = Lists.newArrayList();
    if (scannerOptions.hasStartToken()) {
      bindValues.add(scannerOptions.getStartToken());
    }
    if (scannerOptions.hasStopToken()) {
      bindValues.add(scannerOptions.getStopToken());
    }

    return admin.getPreparedStatement(query).bind(bindValues.toArray());
  }

  /**
   * Create a CQL statement for selecting the columns which makeup the Entity ID from a Cassandra
   * Kiji Table.
   *
   * @param admin Cassandra context object.
   * @param layout table layout of table.
   * @param tableName translated table name as known by Cassandra.
   * @return a statement which will get the single column.
   */
  public static Statement getEntityIDScanStatement(
      CassandraAdmin admin,
      KijiTableLayout layout,
      CassandraTableName tableName) {

    // statement being built:
    //  "SELECT token(${PartitionKeyComponent1}, ${PartitionKeyComponent2} ...), ${EIDColumn1},
    //   ${EIDColumn2}... FROM ${tableName}

    StringBuilder sb = new StringBuilder()
      .append("SELECT token(");

    COMMA_JOINER.appendTo(sb, getPartitionKeyColumns(layout));
    sb.append("), ");
    COMMA_JOINER.appendTo(sb, getEntityIDColumns(layout));
    sb.append(" FROM ")
      .append(tableName);
    String query = sb.toString();

    LOG.debug("Prepared query string for Entity ID scan: {}", query);

    // TODO: ask Clint if going through the admin is necessary in this case.
    return admin.getPreparedStatement(query).bind();
  }

  /**
   * Create a CQL statement to increment the counter in a column.
   *
   * @param admin Cassandra context object.
   * @param layout of table.
   * @param tableName of table.
   * @param entityID of row.
   * @param column to increment.
   * @param amount to increment value.
   * @return a CQL statement to increment a counter column.
   */
  public static Statement getIncrementCounterStatement(
      CassandraAdmin admin,
      KijiTableLayout layout,
      CassandraTableName tableName,
      EntityId entityID,
      CassandraColumnName column,
      long amount
  ) {
    // statement being built:
    //  "UPDATE ${tableName} SET ${VALUE_COL} = ${VALUE_COL} + ?
    //   WHERE ${PKColumn1}=? AND ${PKColumn2}=?...

    StringBuilder sb = new StringBuilder()
      .append("UPDATE ")
      .append(tableName)
      .append(" SET ")
      .append(VALUE_COL)
      .append(" = ")
      .append(VALUE_COL)
      .append(" + ? WHERE ");

    Joiner.on("=? AND ").appendTo(sb, getPrimaryKeyColumns(layout));
    sb.append("=?");

    String query = sb.toString();

    LOG.debug("Prepared query string for counter increment: {}", query);

    List<Object> bindValues = Lists.newArrayList();
    bindValues.add(amount);
    bindValues.addAll(getEntityIDComponentValues(layout, entityID));
    bindValues.add(column.getLocalityGroup());
    bindValues.add(column.getFamilyBuffer());
    bindValues.add(column.getQualifierBuffer());
    bindValues.add(KConstants.CASSANDRA_COUNTER_TIMESTAMP);

    // TODO: ask Clint if going through the admin is necessary in this case.
    return admin.getPreparedStatement(query).bind(bindValues.toArray());
  }

  // CSOFF: ParameterNumberCheck
  /**
   * Create a CQL statement that executes a Kiji put into a non-counter cell.
   *
   * @param admin Cassandra context object.
   * @param layout table layout of table.
   * @param tableName translated table name as known by Cassandra.
   * @param entityId of row to select.
   * @param column to insert into.
   * @param version to write the value at.
   * @param value to be written into column.
   * @param ttl of value, or null if forever.
   * @return a Statement which will execute the insert.
   */
  public static Statement getInsertStatement(
      CassandraAdmin admin,
      KijiTableLayout layout,
      CassandraTableName tableName,
      EntityId entityId,
      CassandraColumnName column,
      Long version,
      ByteBuffer value,
      Integer ttl
  ) {
    // statement being built:
    //  "INSERT INTO ${tableName}
    //   (${PKColumn1}, ${PKColumn2}..., ${VALUE_COL})
    //   VALUES (?, ?..., ?) [USING TTL ${ttl}]

    List<String> columns = getPrimaryKeyColumns(layout);
    columns.add(VALUE_COL);

    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ")
      .append(tableName)
      .append(" (");

    COMMA_JOINER.appendTo(sb, columns);
    sb.append(") VALUES (");
    COMMA_JOINER.appendTo(sb, Collections.nCopies(columns.size(), "?"));
    sb.append(")");

    if (ttl != null && ttl != HConstants.FOREVER) {
      sb.append(" USING TTL ?");
    }

    String query = sb.toString();

    LOG.debug("Prepared query string for insert: {}.", query);

    List<Object> bindValues = Lists.newArrayList();
    bindValues.addAll(getEntityIDComponentValues(layout, entityId));
    bindValues.add(column.getLocalityGroup());
    bindValues.add(column.getFamilyBuffer());
    bindValues.add(column.getQualifierBuffer());
    bindValues.add(version);
    bindValues.add(value);

    if (ttl != null && ttl != HConstants.FOREVER) {
      bindValues.add(ttl);
    }

    return admin.getPreparedStatement(query).bind(bindValues.toArray());
  }
  // CSON

  /**
   * Create a CQL statement to delete a cell.
   *
   * @param admin Cassandra context object.
   * @param layout of table.
   * @param tableName of table.
   * @param entityID of row.
   * @param column containing cell to delete.
   * @param version of cell.
   * @return a CQL statement to delete a cell.
   */
  public static Statement getDeleteCellStatement(
      CassandraAdmin admin,
      KijiTableLayout layout,
      CassandraTableName tableName,
      EntityId entityID,
      CassandraColumnName column,
      long version
  ) {
    Preconditions.checkArgument(column.containsQualifier());
    return getDeleteStatement(
        admin, layout, tableName, entityID, column, version);
  }

  /**
   * Create a CQL statement to delete a column.
   *
   * @param admin Cassandra context object.
   * @param layout of table.
   * @param tableName of table.
   * @param entityID of row.
   * @param column containing column to delete.
   *
   * @return a CQL statement to delete a column.
   */
  public static Statement getDeleteColumnStatement(
      CassandraAdmin admin,
      KijiTableLayout layout,
      CassandraTableName tableName,
      EntityId entityID,
      CassandraColumnName column
  ) {
    return getDeleteStatement(admin, layout, tableName, entityID, column, null);
  }

  /**
   * Create a CQL statement to delete a row.
   *
   * @param admin Cassandra context object.
   * @param layout of table.
   * @param tableName of table.
   * @param entityID of row.
   * @return a CQL statement to delete a row.
   */
  public static Statement getDeleteRowStatement(
      CassandraAdmin admin,
      KijiTableLayout layout,
      CassandraTableName tableName,
      EntityId entityID
  ) {
    return getDeleteStatement(
        admin,
        layout,
        tableName,
        entityID,
        new CassandraColumnName(null, (byte[]) null, null),
        null);
  }

  /**
   * Create a CQL statement for deleting a column from a row of a Cassandra Kiji table.
   *
   * @param admin Cassandra context object.
   * @param layout table layout of table.
   * @param tableName translated table name as known by Cassandra.
   * @param entityId of row to delete from.
   * @param column to delete.  May be unqualified.
   * @param version to delete, or null if all versions.
   * @return a statement which will delete the column.
   */
  private static Statement getDeleteStatement(
      CassandraAdmin admin,
      KijiTableLayout layout,
      CassandraTableName tableName,
      EntityId entityId,
      CassandraColumnName column,
      Long version
  ) {

    // statement being built:
    //  "DELETE FROM ${tableName}
    //   WHERE ${PKColumn1}=? AND ${PKColumn2}=?...
    //   [AND ${LOCALITY_GROUP_COL}=?] [AND ${FAMILY_COL}=?] [AND ${QUALIFIER_COL}=?]
    //   [AND ${VERSION_COL}<=maxVersion]

    List<String> columns = getEntityIDColumns(layout);

    if (column.containsLocalityGroup()) {
      columns.add(LOCALITY_GROUP_COL);
    }
    if (column.containsFamily()) {
      columns.add(FAMILY_COL);
    }
    if (column.containsQualifier()) {
      columns.add(QUALIFIER_COL);
    }
    if (version != null) {
      columns.add(VERSION_COL);
    }

    StringBuilder sb = new StringBuilder()
      .append("DELETE FROM ")
      .append(tableName)
      .append(" WHERE ");

    Joiner.on("=? AND ").appendTo(sb, columns);
    sb.append("=?");

    String query = sb.toString();

    LOG.debug("Prepared query string for delete: {}.", query);

    List<Object> bindValues = Lists.newArrayList();
    bindValues.addAll(getEntityIDComponentValues(layout, entityId));
    if (column.containsLocalityGroup()) {
      bindValues.add(column.getLocalityGroup());
    }
    if (column.containsFamily()) {
      bindValues.add(column.getFamilyBuffer());
    }
    if (column.containsQualifier()) {
      bindValues.add(column.getQualifierBuffer());
    }
    if (version != null) {
      bindValues.add(version);
    }

    return admin.getPreparedStatement(query).bind(bindValues.toArray());
  }
}
