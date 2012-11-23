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

package org.kiji.schema.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Objects;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.client.Delete;

import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseColumnName;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.impl.HBaseEntityId;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;

/**
 * KijiDeletes are used to delete rows or specific columns of a row from Kiji Tables.
 */
public class KijiDelete implements KijiMutation {
  private EntityId mEntityId;
  private String mFamily;
  private String mQualifier;
  private Long mTimestamp;
  private KijiDeleteOperation mOperation;

  /**
   * Helper enum to decide what to do with the Delete.
   */
  public enum KijiDeleteOperation {
    // Exact - version specified by timestamp
    // Upto - version until and including this timestamp
    // Latest - latest version
    // All - every version
    CELL_EXACT,
    CELL_UPTO,
    CELL_ALL,
    CELL_LATEST,
    FAMILY_EXACT,
    FAMILY_UPTO,
    FAMILY_ALL,
    FAMILY_LATEST,
    ROW_ALL,
    ROW_EXACT
  }

  /**
   * Enum used while creating KijiDelete to specify the scope
   * of the operation, i.e. all versions/latest/exact.
   */
  public static enum KijiDeleteScope {
    MULTIPLE_VERSIONS,
    SINGLE_VERSION
  }

  /**
   * Empty constructor for Writable serialization.
   */
  public KijiDelete() { }

  /**
   * Creates a new KijiDelete for all versions of a family.
   * This is not supported on map type families.
   *
   * @param entityId The entityId of the row to delete.
   * @param family The family of the column to delete.
   * @param scope Indication to delete all or latest version.
   */
  public KijiDelete(EntityId entityId, String family, KijiDeleteScope scope) {
    this(entityId, family, null, null, null);
    if (KijiDeleteScope.MULTIPLE_VERSIONS == scope) {
      mOperation = KijiDeleteOperation.FAMILY_ALL;
    } else {
      mOperation = KijiDeleteOperation.FAMILY_LATEST;
    }
  }

  /**
   * Creates a new KijiDelete for all versions of the family up to
   * this timestamp. This is not supported on map type families.
   *
   * @param entityId The entityId of the row to delete.
   * @param family The family of the column to delete.
   * @param timestamp The version of the row to delete.
   * @param scope Indication to delete upto a version or the exact version.
   */
  public KijiDelete(EntityId entityId, String family, Long timestamp, KijiDeleteScope scope) {
    this(entityId, family, null, timestamp, scope);
    if (KijiDeleteScope.MULTIPLE_VERSIONS == scope) {
      mOperation = KijiDeleteOperation.FAMILY_UPTO;
    } else {
      mOperation = KijiDeleteOperation.FAMILY_EXACT;
    }
  }

  /**
   * Creates a new KijiDelete for entire row.
   *
   * @param entityId The entityId of the row to delete.
   */
  public KijiDelete(EntityId entityId) {
    this(entityId, null, null, null, null);
    mOperation = KijiDeleteOperation.ROW_ALL;
  }

  /**
   * Creates a new KijiDelete for a specified row and timestamp.
   *
   * @param entityId The entityId of the row to delete.
   * @param timestamp The version of the row to delete.
   */
  public KijiDelete(EntityId entityId, Long timestamp) {
    this(entityId, null, null, timestamp, null);
    mOperation = KijiDeleteOperation.ROW_EXACT;
  }

  /**
   * Creates a new KijiDelete for all versions of specified columns or the latest
   * version (as specified by scope).
   *
   * @param entityId The entityId of the row to delete.
   * @param family The family of the column to delete
   * @param qualifier The qualifier of the column to delete
   * @param scope The scope of delete, i.e. all versions or latest (specified by exact)
   */
  public KijiDelete(EntityId entityId, String family, String qualifier, KijiDeleteScope scope) {
    this(entityId, family, qualifier, null, scope);
    if (KijiDeleteScope.MULTIPLE_VERSIONS == scope) {
      mOperation = KijiDeleteOperation.CELL_UPTO;
    } else {
      mOperation = KijiDeleteOperation.CELL_LATEST;
    }
  }

  /**
   * Creates a new KijiDelete for a specific column within a family.
   * The scope along with the timestamp decides whether to delete all
   * versions up to this timestamp or only the version specified by timestamp.
   *
   * @param entityId The entityId of the row to delete.
   * @param family The family of the column to delete.
   * @param qualifier The qualifier of the column to delete.
   * @param timestamp The version of the row to delete.
   * @param scope The scope of the delete, i.e. up to or the exact version
   */
  public KijiDelete(EntityId entityId, String family, String qualifier, Long timestamp,
      KijiDeleteScope scope) {
    mEntityId = entityId;
    mFamily = family;
    mQualifier = qualifier;
    mTimestamp = timestamp;
    if (KijiDeleteScope.MULTIPLE_VERSIONS == scope) {
      mOperation = KijiDeleteOperation.CELL_ALL;
    } else {
      mOperation = KijiDeleteOperation.CELL_EXACT;
    }
  }

  /**
   * Gets the entityId of this delete operation.
   *
   * @return The entityId of the data to delete.
   */
  public EntityId getEntityId() {
    return mEntityId;
  }

  /**
   * Gets the family of this delete operation.
   *
   * @return The family of the data to delete.
   */
  public String getFamily() {
    return mFamily;
  }

  /**
   * Gets the qualifier of this delete operation.
   *
   * @return The qualifier of the data to delete.
   */
  public String getQualifier() {
    return mQualifier;
  }

  /**
   * Gets the timestamp of this delete operation.
   *
   * @return The timestamp of the data to delete.
   */
  public long getTimestamp() {
    return mTimestamp;
  }

  /**
   * Gets the scope of cells being deleted.
   *
   * @return The cells to be deleted.
   */
  public KijiDeleteOperation getOperation() {
    return mOperation;
  }

  /**
   * Helper to convert a Kiji cell to an Hbase cell.
   *
   * @param translator The ColumnNameTranslator used to convert kiji column names to their HBase
   *     counterparts.
   * @return An Hbase column name.
   * @throws NoSuchColumnException If the column does not exist.
   */
  private HBaseColumnName convertKijiCelltoHBaseColumn(ColumnNameTranslator translator)
      throws NoSuchColumnException {
    final KijiColumnName kijiColumnName = new KijiColumnName(mFamily, mQualifier);
    final HBaseColumnName hColumn = translator.toHBaseColumnName(kijiColumnName);
    return hColumn;
  }

  /**
   * Helper to converts mFamily to a FamilyLayout object.
   *
   * @param translator The ColumnNameTranslator used to convert kiji column names to their HBase
   *     counterparts.
   * @return A FamilyLayout object representing this mFamily.
   */
  private FamilyLayout getFamilyLayout(ColumnNameTranslator translator) {
    final KijiTableLayout tLayout = translator.getTableLayout();
    return tLayout.getFamilyMap().get(mFamily);
  }

  /**
   * Builds a HBase Delete from this KijiDelete.
   *
   * @param translator The ColumnNameTranslator used to convert kiji column names to their HBase
   *     counterparts.
   * @return An HBase Delete for this KijiDelete.
   * @throws IOException if there is an error building the HBase Delete.
   */
  public Delete toDelete(ColumnNameTranslator translator)
      throws IOException {
    // Build HBase Delete.
    final Delete del;
    if (KijiDeleteOperation.ROW_EXACT == mOperation) {
      del = new Delete(mEntityId.getHBaseRowKey(), mTimestamp, null);
    } else {
      del = new Delete(mEntityId.getHBaseRowKey());
    }

    final HBaseColumnName hColumn;
    final FamilyLayout familyLayout;
    switch (mOperation) {
      // Delete all versions of the specified column.
    case CELL_ALL:
      hColumn = convertKijiCelltoHBaseColumn(translator);
      del.deleteColumns(hColumn.getFamily(), hColumn.getQualifier());
      break;
      // Delete the specified version of the specified column.
    case CELL_EXACT:
      hColumn = convertKijiCelltoHBaseColumn(translator);
      del.deleteColumn(hColumn.getFamily(), hColumn.getQualifier(), mTimestamp);
      break;
      // Delete the latest version of a column
    case CELL_LATEST:
      hColumn = convertKijiCelltoHBaseColumn(translator);
      del.deleteColumn(hColumn.getFamily(), hColumn.getQualifier());
      break;
      // Delete all versions of the specified column with a timestamp
      // less than or equal to the specified timestamp.
    case CELL_UPTO:
      hColumn = convertKijiCelltoHBaseColumn(translator);
      del.deleteColumns(hColumn.getFamily(), hColumn.getQualifier(), mTimestamp);
      break;
      // Delete all versions of all columns in the family
    case FAMILY_ALL:
      familyLayout = getFamilyLayout(translator);
      if (!familyLayout.isMapType()) {
        for (FamilyLayout.ColumnLayout columnLayout: familyLayout.getColumns()) {
          HBaseColumnName hColumnName =
              translator.toHBaseColumnName(new KijiColumnName(mFamily, columnLayout.getName()));
          del.deleteColumns(hColumnName.getFamily(), hColumnName.getQualifier());
        }
      } else {
        throw new UnsupportedOperationException("Deleting map type family "
            + "is not yet supported");
      }
      break;
      // Delete the exact version of all columns in the family as specified by timestamp
    case FAMILY_EXACT:
      familyLayout = getFamilyLayout(translator);
      if (!familyLayout.isMapType()) {
        for (FamilyLayout.ColumnLayout columnLayout: familyLayout.getColumns()) {
          HBaseColumnName hColumnName =
              translator.toHBaseColumnName(new KijiColumnName(mFamily, columnLayout.getName()));
          del.deleteColumn(hColumnName.getFamily(), hColumnName.getQualifier(), mTimestamp);
        }
      } else {
        throw new UnsupportedOperationException("Deleting map type family "
            + "is not yet supported");
      }
      break;
      // Delete the latest version of all columns in the family
    case FAMILY_LATEST:
      familyLayout = getFamilyLayout(translator);
      if (!familyLayout.isMapType()) {
        for (FamilyLayout.ColumnLayout columnLayout: familyLayout.getColumns()) {
          HBaseColumnName hColumnName =
              translator.toHBaseColumnName(new KijiColumnName(mFamily, columnLayout.getName()));
          del.deleteColumn(hColumnName.getFamily(), hColumnName.getQualifier());
        }
      } else {
        throw new UnsupportedOperationException("Deleting map type family "
            + "is not yet supported");
      }
      break;
      // Delete all columns of the specified family with a timestamp
      // less than or equal to the specified timestamp.
    case FAMILY_UPTO:
      familyLayout = getFamilyLayout(translator);
      if (!familyLayout.isMapType()) {
        for (FamilyLayout.ColumnLayout columnLayout: familyLayout.getColumns()) {
          HBaseColumnName hColumnName =
              translator.toHBaseColumnName(new KijiColumnName(mFamily, columnLayout.getName()));
          del.deleteColumns(hColumnName.getFamily(), hColumnName.getQualifier(), mTimestamp);
        }
      } else {
        throw new UnsupportedOperationException("Deleting map type family "
            + "is not yet supported");
      }
      break;
    default:
      // Then it's an operation on the entire row, so we don't need to do specify any
      // column-related information in the HBase Delete.
    }
    return del;
  }

  /**
   * During serialization, if an attribute is null, write the String NULL to DataOutput,
   * else write the actual value.
   *
   * @param toWrite Object to write to DataOutput.
   * @param out Interface to output stream.
   * @throws IOException if an I/O error occurs.
   */
  private void writeOptionalValue(Object toWrite, DataOutput out) throws IOException {
    if (null == toWrite) {
      out.writeUTF("NULL");
      return;
    }
    if (toWrite instanceof KijiDeleteOperation) {
      out.writeUTF(((KijiDeleteOperation) toWrite).name());
      return;
    }
    if (toWrite instanceof Long) {
      out.writeUTF(((Long) toWrite).toString());
      return;
    }
    out.writeUTF((String) toWrite);
  }

  /**
   * During deserialization, read attributes that may have been NULL while serializing.
   *
   * @param in Interface to input stream.
   * @param type The class to read.
   * @return An object of "type" or null.
   * @throws IOException if an I/O error occurs.
   */
  private Object readOptionalValue(DataInput in, Class<?> type) throws IOException {
    String input = in.readUTF();
    if (input.equals("NULL")) {
      return null;
    }
    if (KijiDeleteOperation.class == type) {
      return KijiDeleteOperation.valueOf(input);
    }
    if (Long.class == type) {
      return new Long(input);
    }
    return input;
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    // EntityId.
    final byte[] bytes = mEntityId.getHBaseRowKey();
    out.writeInt(bytes.length);
    out.write(bytes);

    // Family/Qualifier/Timestamp.
    writeOptionalValue(mFamily, out);
    writeOptionalValue(mQualifier, out);
    writeOptionalValue(mTimestamp, out);
    writeOptionalValue(mOperation, out);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    // EntityId.
    final byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    mEntityId = new HBaseEntityId(bytes);

    // Family/Qualifier/Timestamp.
    mFamily = (String)readOptionalValue(in, String.class);

    mQualifier = (String) readOptionalValue(in, String.class);
    mTimestamp = (Long) readOptionalValue(in, Long.class);
    mOperation = (KijiDeleteOperation)readOptionalValue(in, KijiDeleteOperation.class);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (null == other || !(other instanceof KijiDelete)) {
      return false;
    }
    final KijiDelete o = (KijiDelete) other;
    return new EqualsBuilder()
        .append(mEntityId, o.mEntityId)
        .append(mFamily, o.mFamily)
        .append(mQualifier, o.mQualifier)
        .append(mTimestamp, o.mTimestamp)
        .append(mOperation, o.mOperation)
        .isEquals();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(mEntityId)
        .append(mFamily)
        .append(mQualifier)
        .append(mTimestamp)
        .append(mOperation)
        .hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("entityId", mEntityId)
        .add("family", mFamily)
        .add("qualifier", mQualifier)
        .add("timestamp", mTimestamp)
        .add("operation", mOperation)
        .toString();
  }
}
