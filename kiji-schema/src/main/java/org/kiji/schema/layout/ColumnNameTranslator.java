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

package org.kiji.schema.layout;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * Translates between HTable and Kiji table column names.
 *
 * <p>This class defines a mapping between names of HBase HTable families/qualifiers and
 * Kiji table family/qualifiers.</p>
 */
@ApiAudience.Public
public final class ColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnNameTranslator.class);

  /** Used to separate the Kiji family from the Kiji qualifier in an HBase qualifier. */
  public static final String SEPARATOR = ":";

  /** The table to translate names for. */
  private final KijiTableLayout mTableLayout;

  /** A map from ColumnId to its locality group. */
  private final Map<ColumnId, LocalityGroupLayout> mLocalityGroups;

  /**
   * Creates a new <code>ColumnNameTranslator</code> instance.
   *
   * @param tableLayout The layout of the table to translate column names for.
   */
  public ColumnNameTranslator(KijiTableLayout tableLayout) {
    mTableLayout = tableLayout;

    // Index the locality groups by their ColumnIds.
    mLocalityGroups = new HashMap<ColumnId, LocalityGroupLayout>();
    for (Map.Entry<ColumnId, String> entry : mTableLayout.getLocalityGroupIdNameMap().entrySet()) {
      final ColumnId lgId = entry.getKey();
      final String lgName = entry.getValue();
      mLocalityGroups.put(lgId, mTableLayout.getLocalityGroupMap().get(lgName));
    }
  }

  /**
   * Translates an HBase column name to a Kiji column name.
   *
   * @param hbaseColumnName The HBase column name.
   * @return The Kiji column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public KijiColumnName toKijiColumnName(HBaseColumnName hbaseColumnName)
      throws NoSuchColumnException {
    LOG.debug(String.format("Translating HBase column name '%s' to Kiji column name...",
        hbaseColumnName));
    final ColumnId lgId = ColumnId.fromByteArray(hbaseColumnName.getFamily());
    final LocalityGroupLayout localityGroup = mLocalityGroups.get(lgId);
    if (null == localityGroup) {
      throw new NoSuchColumnException(String.format(
          "No locality group with ID/HBase family: '%s'.",
          hbaseColumnName.getFamilyAsString()));
    }

    final String[] parts = StringUtils.splitByWholeSeparatorPreserveAllTokens(
        hbaseColumnName.getQualifierAsString(), SEPARATOR, 2);
    if (2 != parts.length) {
      throw new NoSuchColumnException(
          "Missing separator (" + SEPARATOR + ") from HBase qualifier ("
          + hbaseColumnName.getQualifierAsString()
          + "). Unable to parse Kiji family/qualifier pair.");
    }

    final ColumnId familyId = ColumnId.fromString(parts[0]);
    final FamilyLayout kijiFamily = getKijiFamilyById(localityGroup, familyId);
    if (null == kijiFamily) {
      throw new NoSuchColumnException(String.format(
          "No family with ColumnId '%s' in locality group '%s'.",
          familyId, localityGroup.getDesc().getName()));
    }

    if (kijiFamily.isGroupType()) {
      // Group type family.
      final ColumnId columnId = ColumnId.fromString(parts[1]);
      final ColumnLayout kijiColumn = getKijiColumnById(kijiFamily, columnId);
      if (null == kijiColumn) {
        throw new NoSuchColumnException(String.format(
            "No column with ColumnId '%s' in family '%s'.",
            columnId, kijiFamily.getDesc().getName()));
      }
      final KijiColumnName result =
          new KijiColumnName(kijiFamily.getDesc().getName(), kijiColumn.getDesc().getName());
      LOG.debug(String.format("Translated to Kiji group column '%s'.", result));
      return result;
    }

    // Map type family.
    assert kijiFamily.isMapType();
    final KijiColumnName result = new KijiColumnName(kijiFamily.getDesc().getName(), parts[1]);
    LOG.debug(String.format("Translated to Kiji map column '%s'.", result));
    return result;
  }

  /**
   * Translates a Kiji column name into an HBase column name.
   *
   * @param kijiColumnName The Kiji column name.
   * @return The HBase column name.
   * @throws NoSuchColumnException If the column name cannot be found.
   */
  public HBaseColumnName toHBaseColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException {
    final String familyName = kijiColumnName.getFamily();
    final FamilyLayout fLayout = mTableLayout.getFamilyMap().get(familyName);
    if (null == fLayout) {
      throw new NoSuchColumnException(kijiColumnName.toString());
    }
    final ColumnId lgId = fLayout.getLocalityGroup().getId();
    final ColumnId familyId = fLayout.getId();

    final String qualifier = kijiColumnName.getQualifier();
    if (fLayout.isGroupType()) {
      // Group type family.
      if (null != qualifier) {
        final ColumnId columnId = fLayout.getColumnIdNameMap().inverse().get(qualifier);
        if (null == columnId) {
          throw new NoSuchColumnException(kijiColumnName.toString());
        }

        return new HBaseColumnName(toHBaseFamily(lgId), toHBaseQualifier(familyId, columnId));
      }

      // The caller is attempting to translate a Kiji column name that has only a family,
      // no qualifier.  This is okay.  We'll just return an HBaseColumnName with an empty
      // qualifier suffix.
      return new HBaseColumnName(toHBaseFamily(lgId), toHBaseQualifier(familyId, (String) null));
    } else {
      // Map type family.
      assert fLayout.isMapType();
      return new HBaseColumnName(toHBaseFamily(lgId), toHBaseQualifier(familyId, qualifier));
    }
  }

  /**
   * Gets a Kiji column family from within a locality group by ID.
   *
   * @param localityGroup The locality group to look in.
   * @param familyId The ColumnId of the family to look for.
   * @return The family, or null if no family with the ID can be found within the locality group.
   */
  private FamilyLayout getKijiFamilyById(LocalityGroupLayout localityGroup, ColumnId familyId) {
    final String familyName = localityGroup.getFamilyIdNameMap().get(familyId);
    return localityGroup.getFamilyMap().get(familyName);
  }

  /**
   * Gets a Kiji column from within a group-type family by ID.
   *
   * @param family The group-type family to look in.
   * @param columnId The ColumnId of the column to look for.
   * @return The column, or null if no column with the ID can be found within the family.
   */
  private ColumnLayout getKijiColumnById(FamilyLayout family, ColumnId columnId) {
    final String columnName = family.getColumnIdNameMap().get(columnId);
    return family.getColumnMap().get(columnName);
  }

  /**
   * Turns a Kiji locality group into an HBase family.
   *
   * @param localityGroupId The ColumnID of the locality group.
   * @return The HBase family that should store data in this locality group.
   */
  private static byte[] toHBaseFamily(ColumnId localityGroupId) {
    return localityGroupId.toByteArray();
  }

  /**
   * Turns a group-type family and column into an HBase qualifier.
   *
   * @param familyId The ColumnId of the group-type family.
   * @param columnId The ColumnId of the Kiji column family.
   * @return The HBase qualifier that should store this Kiji column's data.
   */
  private static byte[] toHBaseQualifier(ColumnId familyId, ColumnId columnId) {
    StringBuilder hbaseQualifier = new StringBuilder()
        .append(familyId.toString())
        .append(SEPARATOR)
        .append(columnId.toString());
    return Bytes.toBytes(hbaseQualifier.toString());
  }

  /**
   * Turns a map-type family and key into an HBase qualifier.
   *
   * @param familyId The ColumnId of the map-type family.
   * @param key The key into the map-type family.
   * @return The HBase qualifier that should store this Kiji column's data.
   */
  private static byte[] toHBaseQualifier(ColumnId familyId, String key) {
    StringBuilder hbaseQualifier = new StringBuilder()
        .append(familyId.toString())
        .append(SEPARATOR);
    if (null != key) {
      hbaseQualifier.append(key);
    }
    return Bytes.toBytes(hbaseQualifier.toString());
  }

  /** @return the table layout. */
  public KijiTableLayout getTableLayout() {
    return mTableLayout;
  }
}
