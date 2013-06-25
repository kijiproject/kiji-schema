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
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.ColumnDesc;
import org.kiji.schema.avro.FamilyDesc;
import org.kiji.schema.avro.LocalityGroupDesc;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.avro.TableLayoutDesc;

/**
 * An experimental table layout builder class.
 * Currently manipulates reader, writer, and written schemas.
 */
@ApiAudience.Private
public class TableLayoutBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TableLayoutBuilder.class);

  /** Table layout descriptor builder backing this TableLayoutBuilder. */
  private final TableLayoutDesc.Builder mDescBuilder;

  /** Kiji instance in which to configure schemas. */
  private final Kiji mKiji;

  /**
   * Construct with an existing reference table.
   *
   * @param tableLayoutDesc to build with
   * @param kiji the instance in which to configure schemas
   */
  public TableLayoutBuilder(TableLayoutDesc tableLayoutDesc, Kiji kiji) {
    mKiji = kiji;
    mDescBuilder = TableLayoutDesc.newBuilder(tableLayoutDesc)
        .setReferenceLayout(tableLayoutDesc.getLayoutId())
        .setLayoutId(null);
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
   * Returns a mutable list of registered schemas (READER, WRITER, WRITTEN)
   * of the provided column name.
   * The list of schemas comes from a deeply nested record within the mutable
   * table layout descriptor.
   * Therefore, mutating this list effectively mutates the table layout descriptor.
   *
   * @param columnName whose schema ids to list
   * @param schemaRegistrationType of the schemas to list: (READER, WRITER, WRITTEN).
   * @return The list of schema ids.
   *         Returns empty list if schema validation was not previously enabled.
   * @throws NoSuchColumnException when column not found
   * @throws InvalidLayoutException if the column is final or non-AVRO
   */
  private List<Long> getRegisteredSchemaIds(
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

    // Return mutable schema list.
    switch (schemaRegistrationType) {
      case WRITER: {
        if (null == cellSchema.getWriters()) {
          cellSchema.setWriters(Lists.<Long>newArrayList());
        }
        return cellSchema.getWriters();
      }
      case READER: {
        if (null == cellSchema.getReaders()) {
          cellSchema.setReaders(Lists.<Long>newArrayList());
        }
        return cellSchema.getReaders();
      }
      case WRITTEN: {
        if (null == cellSchema.getWritten()) {
          cellSchema.setWritten(Lists.<Long>newArrayList());
        }
        return cellSchema.getWritten();
      }
      default: {
        throw new IllegalArgumentException(
            "Schema registration type must be READER, WRITER, or WRITTEN: "
            + schemaRegistrationType);
      }
    }
  }

  /**
   * Distinguish reader, writer, and written schema "registration types".
   */
  private static enum SchemaRegistrationType {
    READER,
    WRITER,
    WRITTEN
  }

  /**
   * Register a (READER | WRITER | WRITTEN) schema to a column.
   *
   * @param columnName at which to register the schema.
   * @param schema to register.
   * @param schemaRegistrationType of the schema to register: (READER, WRITER, WRITTEN).
   * @throws IOException If the parameters are invalid.
   * @return this.
   */
  private TableLayoutBuilder withSchema(
      final KijiColumnName columnName,
      final Schema schema,
      final SchemaRegistrationType schemaRegistrationType)
      throws IOException {
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(schemaRegistrationType);

    // Has potential for memory misusage: schemas get added, but never cleaned up.
    final long schemaId = mKiji.getSchemaTable().getOrCreateSchemaId(schema);

    // Update list of schemas.
    final List<Long> schemas = getRegisteredSchemaIds(columnName, schemaRegistrationType);
    if (!schemas.add(schemaId)) {
      LOG.debug("Schema id {} is already registered.", schemaId);
    }

    return this;
  }

  /**
   * Deregister a (READER | WRITER | WRITTEN) schema to a column.
   *
   * @param columnName at which to deregister the schema.
   * @param schema to register.
   * @param schemaRegistrationType of the schema to deregister: (READER, WRITER, WRITTEN).
   * @throws IOException If the parameters are invalid.
   * @return this.
   */
  private TableLayoutBuilder withoutSchema(
      final KijiColumnName columnName,
      final Schema schema,
      final SchemaRegistrationType schemaRegistrationType)
      throws IOException {
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(schemaRegistrationType);

    // Has potential for memory misusage: schemas get added, but never cleaned up.
    final long schemaId = mKiji.getSchemaTable().getOrCreateSchemaId(schema);

    // Update list of schemas.
    final List<Long> schemas = getRegisteredSchemaIds(columnName, schemaRegistrationType);
    if (!schemas.remove(schemaId)) {
      LOG.debug("Schema id {} was not registered.", schemaId);
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
  public List<Schema> getRegisteredSchemas(
      final KijiColumnName columnName,
      final SchemaRegistrationType schemaRegistrationType)
      throws IOException {
    Preconditions.checkNotNull(columnName);
    Preconditions.checkNotNull(schemaRegistrationType);
    final List<Long> schemaIds = getRegisteredSchemaIds(columnName, schemaRegistrationType);
    final List<Schema> schemas = Lists.newLinkedList();
    for (long schemaId : schemaIds) {
      schemas.add(mKiji.getSchemaTable().getSchema(schemaId));
    }
    return schemas;
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
   * Deregister a reader schema to a column.
   *
   * @param columnName at which to deregister the schema.
   * @param schema to deregister.
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
   * Deregister a writer schema to a column.
   *
   * @param columnName at which to deregister the schema.
   * @param schema to deregister.
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
   * Deregister a written schema to a column.
   *
   * @param columnName at which to deregister the schema.
   * @param schema to deregister.
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
   * List registered reader schemas registered at a column.
   *
   * @param columnName whose schemas to list.
   * @return The list of schemas.
   *         Returns empty list if schema validation was not previously enabled.
   * @throws IOException if the cellSchema of the column can not be acquired.
   */
  public List<Schema> getRegisteredReaders(final KijiColumnName columnName)
      throws IOException {
    return getRegisteredSchemas(columnName, SchemaRegistrationType.READER);
  }

  /**
   * List registered writer schemas registered at a column.
   *
   * @param columnName whose schemas to list.
   * @return The list of schemas.
   *         Returns empty list if schema validation was not previously enabled.
   * @throws IOException if the cellSchema of the column can not be acquired.
   */
  public List<Schema> getRegisteredWriters(final KijiColumnName columnName)
      throws IOException {
    return getRegisteredSchemas(columnName, SchemaRegistrationType.WRITER);
  }

  /**
   * List registered written schemas registered at a column.
   *
   * @param columnName whose schemas to list.
   * @return The list of schemas.
   *         Returns empty list if schema validation was not previously enabled.
   * @throws IOException if the cellSchema of the column can not be acquired.
   */
  public List<Schema> getRegisteredWritten(final KijiColumnName columnName)
      throws IOException {
    return getRegisteredSchemas(columnName, SchemaRegistrationType.WRITTEN);
  }

  /**
   * List reader schemas ids registered at a column.
   *
   * @param columnName whose schema ids to list.
   * @return The list of schema ids.
   *         Returns empty list if schema validation was not previously enabled.
   * @throws IOException if the cellSchema of the column can not be acquired.
   */
  public List<Long> getRegisteredReaderIds(final KijiColumnName columnName)
      throws IOException {
    return getRegisteredSchemaIds(columnName, SchemaRegistrationType.READER);
  }

  /**
   * List writer schemas ids registered at a column.
   *
   * @param columnName whose schema ids to list.
   * @return The list of schema ids.
   *         Returns empty list if schema validation was not previously enabled.
   * @throws IOException if the cellSchema of the column can not be acquired.
   */
  public List<Long> getRegisteredWriterIds(final KijiColumnName columnName)
      throws IOException {
    return getRegisteredSchemaIds(columnName, SchemaRegistrationType.WRITER);
  }

  /**
   * List written schemas ids registered at a column.
   *
   * @param columnName whose schema ids to list.
   * @return The list of schema ids.
   *         Returns empty list if schema validation was not previously enabled.
   * @throws IOException if the cellSchema of the column can not be acquired.
   */
  public List<Long> getRegisteredWrittenIds(final KijiColumnName columnName)
      throws IOException {
    return getRegisteredSchemaIds(columnName, SchemaRegistrationType.WRITTEN);
  }

  /**
   * Set the layout id.
   *
   * @param layoutId to set
   */
  public void setLayoutId(String layoutId) {
    mDescBuilder.setLayoutId(layoutId);
  }

  /**
   * Get the layout id.
   *
   * @return layout id
   */
  public String getLayoutId() {
    return mDescBuilder.getLayoutId();
  }
}
