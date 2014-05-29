/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.schema.layout;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.avro.AvroSchema;
import org.kiji.schema.avro.AvroValidationPolicy;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.ColumnDesc;
import org.kiji.schema.avro.FamilyDesc;
import org.kiji.schema.avro.LocalityGroupDesc;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.util.ProtocolVersion;

/**
 * An experimental table layout builder class.
 * Currently manipulates reader, writer, and written schemas.
 *
 * <p> Normalizes table layout descriptors with schema IDs only instead of JSON descriptions. </p>
 */
@ApiAudience.Private
public final class TableLayoutBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TableLayoutBuilder.class);

  /** Table layout descriptor builder backing this TableLayoutBuilder. */
  private final TableLayoutDesc.Builder mDescBuilder;

  /** Table to resolve Avro schemas. */
  private final KijiSchemaTable mSchemaTable;

  /** Function that resolves AvroSchema to Schema objects against the specified Kiji instance. */
  private final AvroSchemaResolver mSchemaResolver;

  /**
   * Initializes a layout builder with no reference layout.
   *
   * @param schemaTable Table to resolve Avro schemas.
   * @throws IOException on I/O error.
   */
  public TableLayoutBuilder(KijiSchemaTable schemaTable) throws IOException {
    mSchemaTable = schemaTable;
    mSchemaResolver = new SchemaTableAvroResolver(schemaTable);
    mDescBuilder = null;
  }

  /**
   * Initializes a new layout builder from an existing reference layout.
   *
   * <p>
   *   Automatically sets a new layout ID computed from the reference layout ID.
   *   Normalizes Avro schemas to use schema UIDs only.
   * </p>
   *
   * @param tableLayoutDesc to build with
   * @param kiji the instance in which to configure schemas
   * @throws IOException on I/O error.
   *     In particular, throws InvalidLayoutException if validation is not supported.
   */
  public TableLayoutBuilder(TableLayoutDesc tableLayoutDesc, Kiji kiji)
      throws IOException {
    final ProtocolVersion layoutVersion = ProtocolVersion.parse(tableLayoutDesc.getVersion());
    if (Versions.LAYOUT_VALIDATION_VERSION.compareTo(layoutVersion) > 0) {
      throw new InvalidLayoutException("Schema validation is available from "
          + Versions.LAYOUT_VALIDATION_VERSION + " and up; this layout is " + layoutVersion);
    }
    mSchemaTable = kiji.getSchemaTable();
    mSchemaResolver = new SchemaTableAvroResolver(mSchemaTable);
    mDescBuilder = TableLayoutDesc.newBuilder(tableLayoutDesc)
        .setReferenceLayout(tableLayoutDesc.getLayoutId())
        .setLayoutId(nextLayoutId(tableLayoutDesc.getLayoutId()));

    // Normalize table layout to use schema UIDs only:
    normalizeTableLayoutDesc(
        mDescBuilder,
        new LayoutOptions()
            .setSchemaFormat(LayoutOptions.SchemaFormat.UID));
  }

  /**
   * Return the corresponding mutable family descriptor of the provided column family.
   * The family descriptor comes from a nested record within the mutable table layout descriptor.
   * Therefore, mutating this family descriptor effectively mutates the table layout descriptor.
   *
   * @param family whole family descriptor to extract
   * @return the family descriptor
   * @throws NoSuchColumnException when family not found
   */
  private FamilyDesc getFamilyDesc(final KijiColumnName family) throws NoSuchColumnException {
    // Traverse through the locality groups to locate family.
    for (LocalityGroupDesc lgd : mDescBuilder.getLocalityGroups()) {
      for (FamilyDesc fd : lgd.getFamilies()) {
        if (fd.getName().equals(family.getFamily())) {
          return fd;
        }
        for (String alias : fd.getAliases()) {
          if (alias.equals(family.getFamily())) {
            return fd;
          }
        }
      }
    }

    throw new NoSuchColumnException(String.format(
        "Table '%s' has no family '%s'.", mDescBuilder.getName(), family.getFamily()));
  }
  /**
   * Return the corresponding mutable cell schema of the provided primary column name.
   * The cell schema comes from a deeply nested record within the mutable table layout descriptor.
   * Therefore, mutating this cell schema effectively mutates the table layout descriptor.
   *
   * @param columnName whose cell schema to extract
   * @return cell schema or null if the column does not exist
   * @throws NoSuchColumnException when column not found
   * @throws InvalidLayoutException when invalid column name is provided
   */
  private CellSchema getColumnSchema(final KijiColumnName columnName)
      throws NoSuchColumnException, InvalidLayoutException {
    final boolean groupType = columnName.isFullyQualified();

    // Get the family descriptor.
    final FamilyDesc soughtFd = getFamilyDesc(columnName);

    // If map type, return map schema.
    if (null != soughtFd.getMapSchema()) {
      if (groupType) {
        throw new InvalidLayoutException("A fully qualified map-type column name was provided.");
      } else {
        return soughtFd.getMapSchema();
      }
    }

    // Examine group type columns to return group type column schema.
    for (ColumnDesc cd : soughtFd.getColumns()) {
      if (cd.getName().equals(columnName.getQualifier())) {
        return cd.getColumnSchema();
      } else {
        for (String alias : cd.getAliases()) {
          if (alias.equals(columnName.getQualifier())) {
            return cd.getColumnSchema();
          }
        }
      }
    }

    throw new NoSuchColumnException(String.format(
        "Table '%s' has no column '%s'.", mDescBuilder.getName(), columnName));
  }

  /**
   * Returns a mutable list of registered schemas (READER, WRITER, WRITTEN) of the provided column.
   *
   * <p>
   *   The list of schemas comes from a deeply nested record within the mutable table layout
   *   descriptor. Therefore, mutating this list effectively mutates the table layout descriptor.
   * </p>
   *
   * @param columnName whose schema ids to list
   * @param schemaRegistrationType of the schemas to list: (READER, WRITER, WRITTEN).
   * @return The list of schema ids.
   *         Returns empty list if schema validation was not previously enabled.
   * @throws NoSuchColumnException when column not found
   * @throws InvalidLayoutException if the column is final or non-AVRO
   */
  private List<AvroSchema> getMutableRegisteredSchemaList(
      final KijiColumnName columnName,
      final SchemaRegistrationType schemaRegistrationType)
      throws NoSuchColumnException, InvalidLayoutException {
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(schemaRegistrationType);

    final CellSchema cellSchema = getColumnSchema(columnName);

    // Avoid tracking schemas final or non-AVRO cells.
    if ((SchemaType.AVRO != cellSchema.getType())
        || (SchemaStorage.FINAL == cellSchema.getStorage())) {
      throw new InvalidLayoutException("Final or non-AVRO column schema cannot be modified.");
    }

    final String fieldName = schemaRegistrationType.getCellSchemaFieldName();
    return (List<AvroSchema>) Preconditions.checkNotNull(cellSchema.get(fieldName));
  }

  /**
   * Distinguish reader, writer, and written schema "registration types".
   */
  private static enum SchemaRegistrationType {
    READER("readers"),
    WRITER("writers"),
    WRITTEN("written");

    /**
     * Returns the CellSchema field name this registration type maps to.
     * @return the CellSchema field name this registration type maps to.
     */
    public String getCellSchemaFieldName() {
      return mCellSchemaFieldName;
    }

    /** Name of the CellSchema field this registration type maps to. */
    private final String mCellSchemaFieldName;

    /**
     * Constructs a registration type with the specified associated CellSchema field name.
     *
     * @param fieldName CellSchema field name to associate to this registration type.
     */
    SchemaRegistrationType(String fieldName) {
      mCellSchemaFieldName = fieldName;
    }
  }

  /**
   * Register a (READER | WRITER | WRITTEN) schema to a column.
   *
   * <p> The Schema is added even if it already exists. </p>
   *
   * @param columnName at which to register the schema.
   * @param schema to register.
   * @param schemaRegistrationType of the schema to register: (READER, WRITER, WRITTEN).
   * @throws IOException If the parameters are invalid.
   * @return this builder.
   */
  private TableLayoutBuilder withSchema(
      final KijiColumnName columnName,
      final Schema schema,
      final SchemaRegistrationType schemaRegistrationType)
      throws IOException {
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(schemaRegistrationType);

    final long schemaId = mSchemaTable.getOrCreateSchemaId(schema);
    final List<AvroSchema> schemas =
        getMutableRegisteredSchemaList(columnName, schemaRegistrationType);
    schemas.add(AvroSchema.newBuilder().setUid(schemaId).build());
    return this;
  }

  /**
   * Unregister a (READER | WRITER | WRITTEN) schema to a column.
   *
   * <p> Remove all instances of the specified schema in the list. </p>
   *
   * @param columnName at which to unregister the schema.
   * @param schema to register.
   * @param schemaRegistrationType of the schema to unregister: (READER, WRITER, WRITTEN).
   * @throws IOException If the parameters are invalid.
   * @return this builder.
   */
  private TableLayoutBuilder withoutSchema(
      final KijiColumnName columnName,
      final Schema schema,
      final SchemaRegistrationType schemaRegistrationType)
      throws IOException {
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(schemaRegistrationType);

    final List<AvroSchema> schemas =
        getMutableRegisteredSchemaList(columnName, schemaRegistrationType);
    final Iterator<AvroSchema> it = schemas.iterator();
    while (it.hasNext()) {
      final AvroSchema avroSchema = it.next();
      final Schema other = mSchemaResolver.apply(avroSchema);
      if (Objects.equal(schema, other)) {
        it.remove();
      }
    }
    return this;
  }

  /**
   * List (READER | WRITER | WRITTEN) schemas registered at a column.
   *
   * @param columnName whose schemas to list.
   * @param schemaRegistrationType of the schemas to list: (READER, WRITER, WRITTEN).
   * @return The list of schemas.
   *         Returns empty list if schema validation was not previously enabled.
   * @throws IOException if the cellSchema of the column can not be acquired.
   */
  public Collection<Schema> getRegisteredSchemas(
      final KijiColumnName columnName,
      final SchemaRegistrationType schemaRegistrationType)
      throws IOException {
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(schemaRegistrationType);
    final List<AvroSchema> avroSchemas =
        getMutableRegisteredSchemaList(columnName, schemaRegistrationType);
    return Collections2.transform(avroSchemas, mSchemaResolver);
  }

  /**
   * Build a new table layout descriptor.
   *
   * @return a built table layout descriptor.
   */
  public TableLayoutDesc build() {
    return mDescBuilder.build();
  }

  /**
   * Register a reader schema to a column.
   *
   * @param columnName at which to register the schema.
   * @param schema to register.
   * @throws IOException If the parameters are invalid.
   * @return this.
   */
  public TableLayoutBuilder withReader(
      final KijiColumnName columnName,
      final Schema schema)
      throws IOException {
    return withSchema(columnName, schema, SchemaRegistrationType.READER);
  }

  /**
   * Register a writer schema to a column.
   *
   * @param columnName at which to register the schema.
   * @param schema to register.
   * @throws IOException If the parameters are invalid.
   * @return this.
   */
  public TableLayoutBuilder withWriter(
      final KijiColumnName columnName,
      final Schema schema)
      throws IOException {
    return withSchema(columnName, schema, SchemaRegistrationType.WRITER);
  }

  /**
   * Register a written schema to a column.
   *
   * @param columnName at which to register the schema.
   * @param schema to register.
   * @throws IOException If the parameters are invalid.
   * @return this.
   */
  public  TableLayoutBuilder withWritten(
      final KijiColumnName columnName,
      final Schema schema)
      throws IOException {
    return withSchema(columnName, schema, SchemaRegistrationType.WRITTEN);
  }

  /**
   * Unregister a reader schema to a column.
   *
   * @param columnName at which to unregister the schema.
   * @param schema to unregister.
   * @throws IOException If the parameters are invalid.
   * @return this.
   */
  public TableLayoutBuilder withoutReader(
      final KijiColumnName columnName,
      final Schema schema)
      throws IOException {
    return withoutSchema(columnName, schema, SchemaRegistrationType.READER);
  }

  /**
   * Unregister a writer schema to a column.
   *
   * @param columnName at which to unregister the schema.
   * @param schema to unregister.
   * @throws IOException If the parameters are invalid.
   * @return this.
   */
  public TableLayoutBuilder withoutWriter(
      final KijiColumnName columnName,
      final Schema schema)
      throws IOException {
    return withoutSchema(columnName, schema, SchemaRegistrationType.WRITER);
  }

  /**
   * Unregister a written schema to a column.
   *
   * @param columnName at which to unregister the schema.
   * @param schema to unregister.
   * @throws IOException If the parameters are invalid.
   * @return this.
   */
  public TableLayoutBuilder withoutWritten(
      final KijiColumnName columnName,
      final Schema schema)
      throws IOException {
    return withoutSchema(columnName, schema, SchemaRegistrationType.WRITTEN);
  }

  /**
   * Lists the reader schemas registered for the specified column.
   *
   * @param columnName whose schemas to list.
   * @return The list of schemas.
   *     Returns empty list if schema validation was not previously enabled.
   * @throws IOException if the cellSchema of the column can not be acquired.
   */
  public Collection<Schema> getRegisteredReaders(final KijiColumnName columnName)
      throws IOException {
    return getRegisteredSchemas(columnName, SchemaRegistrationType.READER);
  }

  /**
   * Lists the writer schemas registered for the specified column.
   *
   * @param columnName whose schemas to list.
   * @return The list of schemas.
   *     Returns empty list if schema validation was not previously enabled.
   * @throws IOException if the cellSchema of the column can not be acquired.
   */
  public Collection<Schema> getRegisteredWriters(final KijiColumnName columnName)
      throws IOException {
    return getRegisteredSchemas(columnName, SchemaRegistrationType.WRITER);
  }

  /**
   * List the written schemas recorded for the specified column.
   *
   * @param columnName whose schemas to list.
   * @return The list of schemas.
   *     Returns empty list if schema validation was not previously enabled.
   * @throws IOException if the cellSchema of the column can not be acquired.
   */
  public Collection<Schema> getRegisteredWritten(final KijiColumnName columnName)
      throws IOException {
    return getRegisteredSchemas(columnName, SchemaRegistrationType.WRITTEN);
  }

  /**
   * Set the layout id.
   *
   * @param layoutId to set
   * @return this.
   */
  public TableLayoutBuilder withLayoutId(String layoutId) {
    mDescBuilder.setLayoutId(layoutId);
    return this;
  }

  /**
   * Get the layout id.
   *
   * @return layout id
   */
  public String getLayoutId() {
    return mDescBuilder.getLayoutId();
  }

  /**
   * Set the Avro validation policy for a given column in this table.
   *
   * @param column the column whose Avro validation policy will be set.
   * @param policy the AvroValidationPolicy to set for the given column.
   * @return this builder.
   * @throws InvalidLayoutException in case a fully qualified column in a map family is specified.
   * @throws NoSuchColumnException in case the specified column does not exist.
   */
  public TableLayoutBuilder withAvroValidationPolicy(
      final KijiColumnName column,
      final AvroValidationPolicy policy)
      throws NoSuchColumnException, InvalidLayoutException {
    getColumnSchema(column).setAvroValidationPolicy(policy);
    return this;
  }

  /**
   * Get the Avro validation policy for a given column.
   *
   * @param column the column for which to get the Avro validation policy.
   * @return the Avro validation policy for the given column.
   * @throws NoSuchColumnException in case the column does not exist.
   * @throws InvalidLayoutException in case a fully qualified column in a map family is specified.
   */
  public AvroValidationPolicy getAvroValidationPolicy(final KijiColumnName column)
      throws NoSuchColumnException, InvalidLayoutException {
    return getColumnSchema(column).getAvroValidationPolicy();
  }

  /**
   * Options to normalize table layout descriptors.
   */
  public static final class LayoutOptions {
    /** Ways to represent an Avro schema. */
    public static enum SchemaFormat {
      /** Compact but non-portable way to represent an Avro schema. */
      UID,

      /** Verbose but portable way to represent an Avro schema. */
      JSON;
    };

    private SchemaFormat mSchemaFormat = SchemaFormat.JSON;

    /**
     * Default options to normalize table layout descriptors.
     */
    public LayoutOptions() {
      mSchemaFormat = SchemaFormat.JSON;  // verbose but portable
    }

    /**
     * Sets the format to use to encode Avro schema.
     * @param schemaFormat Format to use to encode Avro schema.
     * @return this builder.
     */
    public LayoutOptions setSchemaFormat(SchemaFormat schemaFormat) {
      mSchemaFormat = schemaFormat;
      return this;
    }

    /**
     * Returns the format to use to encode Avro schema.
     * @return the format to use to encode Avro schema.
     */
    public SchemaFormat getSchemaFormat() { return mSchemaFormat; }
  };

  /**
   * Normalizes a table layout descriptor to use schema UIDs only.
   *
   * @param desc Table layout descriptor to normalize.
   * @param options Options describing the normalization to apply.
   * @return the normalized table layout descriptor.
   * @throws IOException on I/O error.
   */
  public TableLayoutDesc normalizeTableLayoutDesc(TableLayoutDesc desc, LayoutOptions options)
      throws IOException {
    final TableLayoutDesc.Builder descBuilder = TableLayoutDesc.newBuilder(desc);
    normalizeTableLayoutDesc(descBuilder, options);
    return descBuilder.build();
  }

  /**
   * Normalizes the table layout descriptor to use schema UIDs only.
   *
   * @param descBuilder Builder for the table layout descriptor to normalize.
   * @param options Options describing the normalization to apply.
   * @throws IOException on I/O error.
   */
  private void normalizeTableLayoutDesc(TableLayoutDesc.Builder descBuilder, LayoutOptions options)
      throws IOException {
    for (LocalityGroupDesc lgdesc : descBuilder.getLocalityGroups()) {
      for (FamilyDesc fdesc : lgdesc.getFamilies()) {
        if (fdesc.getMapSchema() != null) {
          normalizeCellSchema(fdesc.getMapSchema(), options);
        } else {
          for (ColumnDesc cdesc : fdesc.getColumns()) {
            normalizeCellSchema(cdesc.getColumnSchema(), options);
          }
        }
      }
    }
  }

  /**
   * Normalizes a CellSchema to use schema UIDs only.
   *
   * @param cellSchema CellSchema to normalize.
   * @param options Options describing the normalization to apply.
   * @throws IOException on I/O error.
   */
  private void normalizeCellSchema(CellSchema cellSchema, LayoutOptions options)
      throws IOException {
    switch (cellSchema.getType()) {
      case AVRO: {
        // Ensure the lists of schemas are initialized (non null) and normalized to UIDs:
        if (cellSchema.getReaders() == null) {
          cellSchema.setReaders(Lists.<AvroSchema>newArrayList());
        }
        if (cellSchema.getWriters() == null) {
          cellSchema.setWriters(Lists.<AvroSchema>newArrayList());
        }
        if (cellSchema.getWritten() == null) {
          cellSchema.setWritten(Lists.<AvroSchema>newArrayList());
        }

        if (cellSchema.getDefaultReader() != null) {
          normalizeAvroSchema(cellSchema.getDefaultReader(), options);
        }
        normalizeAvroSchemaCollection(cellSchema.getReaders(), options);
        normalizeAvroSchemaCollection(cellSchema.getWriters(), options);
        normalizeAvroSchemaCollection(cellSchema.getWritten(), options);

        break;
      }
      case CLASS:
      case INLINE:
      case COUNTER:
      case PROTOBUF:
      case RAW_BYTES:
        break;
      default:
          throw new InvalidLayoutException(String.format(
              "Unsupported cell type: %s from CellSchema '%s'.",
              cellSchema.getType(), cellSchema));
    }
  }

  /**
   * Normalizes a collection of AvroSchema to use schema UIDs only.
   *
   * @param avroSchemas Collection of AvroSchema to normalize.
   * @param options Options describing the normalization to apply.
   * @throws IOException on I/O error.
   */
  private void normalizeAvroSchemaCollection(
      Collection<AvroSchema> avroSchemas,
      LayoutOptions options)
      throws IOException {
    for (AvroSchema avroSchema : avroSchemas) {
      normalizeAvroSchema(avroSchema, options);
    }
  }

  /**
   * Normalizes an AvroSchema to use schema UIDs only.
   *
   * <p> Mutates the AvroSchema in-place. </p>
   *
   * @param avroSchema AvroSchema to normalize.
   * @param options Options describing the normalization to apply.
   * @throws IOException on I/O error.
   */
  private void normalizeAvroSchema(AvroSchema avroSchema, LayoutOptions options)
      throws IOException {
    final boolean hasJson = (avroSchema.getJson() != null);
    final boolean hasUid = (avroSchema.getUid() != null);

    /** Schema this descriptor resolves to. */
    Schema schema = null;

    if (hasJson && hasUid) {
      // Both JSON and UID are filled in, this should not happen, but let's handle it:
      schema = new Schema.Parser().parse(avroSchema.getJson());
      final Schema schemaFromId = mSchemaTable.getSchema(avroSchema.getUid());
      if (!Objects.equal(schema, schemaFromId)) {
        throw new InvalidLayoutException(String.format(
            "Inconsistent AvroSchema: '%s' : UID %d does not match with JSON descriptor '%s'.",
            avroSchema, avroSchema.getUid(), avroSchema.getJson()));
      }

    } else if (hasJson) {
      schema = new Schema.Parser().parse(avroSchema.getJson());

    } else if (hasUid) {
      schema = mSchemaTable.getSchema(avroSchema.getUid());

    } else {
      throw new InvalidLayoutException("AvroSchema has neither JSON nor UID: " + avroSchema);
    }

    Preconditions.checkState(schema != null);

    switch (options.getSchemaFormat()) {
      case UID: {
        final long schemaId = mSchemaTable.getOrCreateSchemaId(schema);
        avroSchema.setUid(schemaId);
        avroSchema.setJson(null);
        break;
      }
      case JSON: {
        avroSchema.setUid(null);
        avroSchema.setJson(schema.toString());
        break;
      }
      default: throw new InternalKijiError("Unknown schema format: " + options.getSchemaFormat());
    }
  }

  /**
   * Computes the layout ID directly following a given layout ID.
   *
   * <p>
   *   Increments the layout ID if it is an integer.
   *   Otherwise, forge a layout ID containing a timestamp.
   * </p>
   *
   * @param layoutId Layout ID to compute the next sequential ID from.
   * @return the next sequential layout ID.
   */
  private static String nextLayoutId(String layoutId) {
    try {
      final long lid = Long.parseLong(layoutId);
      return Long.toString(lid + 1);
    } catch (NumberFormatException nfe) {
      return String.format("layout-with-timestamp-%d", System.currentTimeMillis());
    }
  }
}
