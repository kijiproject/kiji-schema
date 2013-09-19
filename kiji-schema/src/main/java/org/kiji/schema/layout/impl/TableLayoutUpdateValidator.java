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

package org.kiji.schema.layout.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.avro.AvroSchema;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.layout.AvroSchemaResolver;
import org.kiji.schema.layout.InvalidLayoutSchemaException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.SchemaTableAvroResolver;
import org.kiji.schema.util.AvroUtils;
import org.kiji.schema.util.AvroUtils.SchemaCompatibilityType;
import org.kiji.schema.util.AvroUtils.SchemaPairCompatibility;
import org.kiji.schema.util.AvroUtils.SchemaSetCompatibility;
import org.kiji.schema.util.ProtocolVersion;

/**
 * Validates a table layout update.
 *
 * <p>
 *   On a column by column basis, ensures that records written by any registered or previously
 *   written writer schema can be read without error by all registered reader schemas.
 * </p>
 */
@ApiAudience.Private
public final class TableLayoutUpdateValidator {
  private static final Logger LOG = LoggerFactory.getLogger(TableLayoutUpdateValidator.class);

  private final Kiji mKiji;
  private final KijiSchemaTable mSchemaTable;

  /**
   * Initializes a TableLayoutUpdateValidator which interprets schema IDs from the schema table of
   * the given Kiji instance.
   *
   * @param kiji the Kiji instance from which to get schema definitions.
   * @throws IOException in case of an error reading from the schema table.
   */
  public TableLayoutUpdateValidator(Kiji kiji) throws IOException {
    mKiji = kiji;
    mSchemaTable = mKiji.getSchemaTable();
  }

  /**
   * Validates a new table layout against a reference layout for mutual compatibility.
   *
   * @param reference the reference layout against which to validate.
   * @param layout the new layout to validate.
   * @throws IOException in case of an IO Error reading from the schema table.
   *     Throws InvalidLayoutException if the layouts are incompatible.
   */
  public void validate(KijiTableLayout reference, KijiTableLayout layout) throws IOException {

    final ProtocolVersion layoutVersion = ProtocolVersion.parse(layout.getDesc().getVersion());

    if (layoutVersion.compareTo(Versions.LAYOUT_VALIDATION_VERSION) < 0) {
      // Layout versions older than layout-1.3.0 do not require validation
      return;
    }

    // Accumulator for error messages which will be used to create an exception if errors occur.
    final List<String> incompatabilityMessages = Lists.newArrayList();

    // Iterate through all families/columns in the new layout,
    // find a potential matching reference family/column,
    // and validate the reader/writer schema sets.
    // If no matching family/column exists in the reference layout the newly create column is valid.
    for (FamilyLayout flayout : layout.getFamilies()) {
      final ColumnId lgid = flayout.getLocalityGroup().getId();

      LocalityGroupLayout refLGLayout = null;
      if (reference != null) {
        // If there is a reference layout, check for a locality group matching the ID of the LG for
        // this family.  Locality Group IDs should not change between layouts.
        final String refLGName = reference.getLocalityGroupIdNameMap().get(lgid);
        if (refLGName != null) {
          // If there is a matching reference LG get its layout by name.
          refLGLayout = reference.getLocalityGroupMap().get(refLGName);
        }
      }

      // The ColumnId of the FamilyLayout from the table layout.  Also matches the FamilyLayout for
      // this family in the reference layout if present.
      final ColumnId familyId = flayout.getId();

      if (flayout.isMapType()) {
        // If the family is map-type, get the CellSchema for all values in the family.
        final CellSchema cellSchema = flayout.getDesc().getMapSchema();

        FamilyLayout refFamilyLayout = null;
        if (refLGLayout != null) {
          // If there is a matching reference LG, check for the existence of this family.
          final String refFamilyName = refLGLayout.getFamilyIdNameMap().get(familyId);
          if (refFamilyName != null) {
            refFamilyLayout = refLGLayout.getFamilyMap().get(refFamilyName);
          }
        }

        if (refFamilyLayout != null) {
          if (refFamilyLayout.isMapType()) {
            // If the FamilyLayout from both table layouts are map type, compare their CellSchemas.
            final CellSchema refCellSchema = refFamilyLayout.getDesc().getMapSchema();

            incompatabilityMessages.addAll(addColumnNamestoIncompatibilityMessages(
                flayout.getName(), null, validateCellSchema(refCellSchema, cellSchema)));
          } else if (refFamilyLayout.isGroupType()) {
            // If the FamilyLayout changed from group-type to map-type between table layout versions
            // that is an incompatible change.
            incompatabilityMessages.add(String.format(
                "Family: %s changed from group-type to map-type.", refFamilyLayout.getName()));
          } else {
            throw new InternalKijiError(String.format(
                "Family: %s is neither map-type nor group-type.", refFamilyLayout.getName()));
          }
        } else {
          // If the reference FamilyLayout is null this indicates a new family, which is inherently
          // compatible, but we still have to validate that the new readers and writers are
          // internally compatible.
          incompatabilityMessages.addAll(addColumnNamestoIncompatibilityMessages(
              flayout.getName(), null, validateCellSchema(null, cellSchema)));
        }
      } else if (flayout.isGroupType()) {
        // Check for a matching family from the reference layout.
        FamilyLayout refFamilyLayout = null;
        if (refLGLayout != null) {
          final String refFamilyName = refLGLayout.getFamilyIdNameMap().get(familyId);
          if (refFamilyName != null) {
            refFamilyLayout = refLGLayout.getFamilyMap().get(refFamilyName);
          }
        }

        if (refFamilyLayout != null) {
          if (refFamilyLayout.isGroupType()) {
            // If there is a matching reference family and it is the same family type, iterate
            // through the columns checking schema compatibility.  Only checks columns from the new
            // layout because removed columns are inherently valid.
            for (ColumnLayout columnLayout : flayout.getColumns()) {
              final CellSchema cellSchema = columnLayout.getDesc().getColumnSchema();

              final String refColumnName =
                  refFamilyLayout.getColumnIdNameMap().get(columnLayout.getId());
              ColumnLayout refColumnLayout = null;
              if (refColumnName != null) {
                // If there is a column from the reference layout with the same column ID, get its
                // layout.
                refColumnLayout = refFamilyLayout.getColumnMap().get(refColumnName);
              }
              // If there is a column from the reference layout with the same column ID, get its
              // CellSchema.
              final CellSchema refCellSchema =
                  (refColumnLayout == null) ? null : refColumnLayout.getDesc().getColumnSchema();

              // If there is no matching column, refCellSchema will be null and this will only test
              // that the new reader and writer schemas are internally compatible.
              incompatabilityMessages.addAll(addColumnNamestoIncompatibilityMessages(
                  flayout.getName(),
                  columnLayout.getName(),
                  validateCellSchema(refCellSchema, cellSchema)));
            }
          } else if (refFamilyLayout.isMapType()) {
            // If the FamilyLayout changed from map-type to group-type between table layout versions
            // that is an incompatible change.
            incompatabilityMessages.add(String.format(
                "Family: %s changed from map-type to group-type.", refFamilyLayout.getName()));
          } else {
            throw new InternalKijiError(String.format(
                "Family: %s is neither map-type nor group-type.", refFamilyLayout.getName()));
          }
        } else {
          // If the reference FamilyLayout is null this indicates a new family, which is inherently
          // compatible, but we still have to validate that the new readers and writers are
          // internally compatible.
          for (ColumnLayout columnLayout : flayout.getColumns()) {
            final CellSchema cellSchema = columnLayout.getDesc().getColumnSchema();
            incompatabilityMessages.addAll(addColumnNamestoIncompatibilityMessages(
                flayout.getName(), columnLayout.getName(), validateCellSchema(null, cellSchema)));
          }
        }

      } else {
        throw new InternalKijiError(String.format(
            "Family: %s is neither map-type nor group-type.", flayout.getName()));
      }
    }

    // If there were any incompatibility errors, throw an exception.
    if (incompatabilityMessages.size() != 0) {
      throw new InvalidLayoutSchemaException(incompatabilityMessages);
    }
  }

  /**
   * Validates a new column specification against its reference specification.
   *
   * @param refCellSchema Optional reference specification. Null means no reference.
   * @param cellSchema New column specification to validate.
   * @return a list of error messages corresponding to each invalid combination of a reader and
   *     writer schema.  Returns an empty list if all reader and writer schemas are compatible.
   * @throws IOException in case of an error reading from the schema table.
   *     Throws InvalidLayoutException in case a schema does not exist in the schema table.
   */
  private List<String> validateCellSchema(
      final CellSchema refCellSchema,
      final CellSchema cellSchema)
      throws IOException {

    switch (cellSchema.getAvroValidationPolicy()) {
      case NONE: {
        // Nothing to validate for this validation policy.
        return Collections.emptyList();
      }
      case SCHEMA_1_0: {
        // system-1.0 compatibility mode doesn't validate Avro schema changes.
        return Collections.emptyList();
      }
      case DEVELOPER:
      case STRICT: {
        // In developer and strict validation policy, we enforce compatibility between
        // reader and writer schemas.  All records written by previous writer schemas or writable
        // by currently registered writer schemas must be readable by all registered reader schemas.
        // Because there will be a transition period in which both layouts are valid, writer schemas
        // which have never been written and which are being removed, must still be validated
        // against new reader schemas and reader schemas which are being removed must still be
        // validated against new writer schemas.

        final Set<AvroSchema> readerSchemaIDs = Sets.newHashSet();
        final Set<AvroSchema> writerSchemaIDs = Sets.newHashSet();

        if (cellSchema.getReaders() != null) {
          readerSchemaIDs.addAll(cellSchema.getReaders());
        }
        if (cellSchema.getWriters() != null) {
            writerSchemaIDs.addAll(cellSchema.getWriters());
        }
        if (cellSchema.getWritten() != null) {
          writerSchemaIDs.addAll(cellSchema.getWritten());
        }

        if (refCellSchema != null) {
          // Merge (union) with the reference reader/writer schemas:
          if (refCellSchema.getReaders() != null) {
            readerSchemaIDs.addAll(refCellSchema.getReaders());
          }
          if (refCellSchema.getWriters() != null) {
            writerSchemaIDs.addAll(refCellSchema.getWriters());
          }
          if (refCellSchema.getWritten() != null) {
            writerSchemaIDs.addAll(refCellSchema.getWritten());
          }
        }

        final AvroSchemaResolver resolver = new SchemaTableAvroResolver(mKiji.getSchemaTable());

        // Resolve reader Avro schema descriptors to Schema objects:
        final List<Schema> readerSchemas =
            Lists.newArrayList(Collections2.transform(readerSchemaIDs, resolver));

        // Resolve writer Avro schema descriptors to Schema objects:
        final List<Schema> writerSchemas =
            Lists.newArrayList(Collections2.transform(writerSchemaIDs, resolver));

        // Perform actual validation between reader and writer schemas:
        return validateReaderWriterSchemas(readerSchemas, writerSchemas);
      }
      default:
        throw new InternalKijiError(
            "Unknown validation policy: " + cellSchema.getAvroValidationPolicy());
    }
  }

  /**
   * Validates each of a set of reader schemas against each of a set of writer schemas.
   *
   * @param readerSchemas the set of reader schemas to validate.
   * @param writerSchemas the set of writer schemas to validate.
   * @return a list of error messages corresponding to each invalid combination of a reader and
   *     writer schema.  Returns an empty list if all reader and writers schemas are compatible.
   */
  private static List<String> validateReaderWriterSchemas(
      List<Schema> readerSchemas,
      List<Schema> writerSchemas) {

    final List<String> incompatabilityMessages = Lists.newArrayList();

    for (Schema readerSchema : readerSchemas) {
      // Check that each reader schema can read records written by all writer schemas.
      final SchemaSetCompatibility compat =
          AvroUtils.checkReaderCompatibility(readerSchema, writerSchemas.iterator());
      if (compat.getType() == SchemaCompatibilityType.INCOMPATIBLE) {
        for (SchemaPairCompatibility pairCompat : compat.getCauses()) {
          if (pairCompat.getType() == SchemaCompatibilityType.INCOMPATIBLE) {
            incompatabilityMessages.add(
                String.format("Reader schema: %s is incompatible with writer schema: %s.",
                pairCompat.getReader(), pairCompat.getWriter()));
          }
        }
      }
    }

    return incompatabilityMessages;
  }

  /**
   * Adds a column annotation to the beginning of incompatibility messages.
   *
   * @param family the family of the column to add.
   * @param qualifier the qualifier of the column to add.  Null indicates an unqualified family.
   * @param incompatibilityMessages the incompatibility messages to which to add column names.
   * @return a list of newly modified incompatibility messages.
   */
  private static List<String> addColumnNamestoIncompatibilityMessages(
      String family, String qualifier, List<String> incompatibilityMessages) {
    Preconditions.checkNotNull(family, "Column family may not be null.");
    final String column = (qualifier != null) ? family + ":" + qualifier : family;
    final List<String> messages = Lists.newArrayList();
    for (String message : incompatibilityMessages) {
      messages.add(String.format("In column: '%s' %s", column, message));
    }
    return messages;
  }
}
