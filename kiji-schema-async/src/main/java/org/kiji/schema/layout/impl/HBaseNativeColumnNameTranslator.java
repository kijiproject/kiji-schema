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

package org.kiji.schema.layout.impl;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

/**
 * Translates between HTable and Kiji table column names.
 *
 * <p>This class defines a mapping between names of Kiji table family/qualifiers and HBase HTable
 * families/qualifiers.  The native mapping will use the Kiji family as the
 * HBase column family and the Kiji qualifier as the HBase qualifier.  Kiji locality groups
 * MUST be specified to be the same as the Kiji column family.</p>
 *
 * <p>This class is for the purpose of reading native HBase tables whose HBase
 * family/qualifiers do not use the notion of locality groups.</p>
 */
@ApiAudience.Private
public final class HBaseNativeColumnNameTranslator extends KijiColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseNativeColumnNameTranslator.class);

  /** The table to translate names for. */
  private final KijiTableLayout mTableLayout;

  /**
   * Creates a new <code>IdentityColumnNameTranslator</code> instance.
   *
   * @param tableLayout The layout of the table to translate column names for.
   */
  public HBaseNativeColumnNameTranslator(KijiTableLayout tableLayout) {
    // Validate that all Kiji column families are the same as their respective locality groups.
    for (LocalityGroupLayout localityGroupLayout : tableLayout.getLocalityGroups()) {
      for (FamilyLayout familyLayout : localityGroupLayout.getFamilies()) {
        Preconditions.checkState(familyLayout.getName().equals(localityGroupLayout.getName()),
            "For HBASE_NATIVE column name translation, "
            + "family: '{}' must match locality group: '{}'",
            familyLayout.getName(), localityGroupLayout.getName());

        // We don't support map-type families in the HBASE_NATIVE layout
        Preconditions.checkState(familyLayout.isGroupType(),
            "For HBASE_NATIVE column name translation, family: '{}' must be a group type.",
            familyLayout.getName());
      }
    }
    mTableLayout = tableLayout;
  }

  /** {@inheritDoc} */
  @Override
  public KijiColumnName toKijiColumnName(HBaseColumnName hbaseColumnName)
      throws NoSuchColumnException {
    LOG.debug("Translating HBase column name '{}' to Kiji column name...", hbaseColumnName);

    // Validate that this family exists within the layout
    String family = hbaseColumnName.getFamilyAsString();
    final FamilyLayout kijiFamily = mTableLayout.getFamilyMap().get(family);
    if (null == kijiFamily) {
      throw new NoSuchColumnException(String.format(
          "No family '%s' in layout.", family));
    }

    // Validate that the qualifier exists within the layout
    if (kijiFamily.isGroupType()) {
      // Group type family.
      String column = hbaseColumnName.getQualifierAsString();
      final ColumnLayout kijiColumn = kijiFamily.getColumnMap().get(column);
      if (null == kijiColumn) {
        throw new NoSuchColumnException(String.format(
            "No column with ColumnId '%s' in family '%s'.",
            column, kijiFamily.getDesc().getName()));
      }
    }

    final KijiColumnName result = new KijiColumnName(hbaseColumnName.getFamilyAsString(),
        hbaseColumnName.getQualifierAsString());
    LOG.debug("Translated to Kiji group column '{}'.", result);
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public HBaseColumnName toHBaseColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException {

    // Validate that this family exists within the layout
    String family = kijiColumnName.getFamily();
    final FamilyLayout kijiFamily = mTableLayout.getFamilyMap().get(family);
    if (null == kijiFamily) {
      throw new NoSuchColumnException(String.format(
          "No family '%s' in layout.", family));
    }

    // Validate that the qualifier exists within the layout
    if (kijiFamily.isGroupType()) {
      // Group type family.
      String column = kijiColumnName.getQualifier();
      final ColumnLayout kijiColumn = kijiFamily.getColumnMap().get(column);
      if (null == kijiColumn) {
        throw new NoSuchColumnException(String.format(
            "No column with ColumnId '%s' in family '%s'.",
            column, kijiFamily.getDesc().getName()));
      }
    }

    // Map the Kiji column family and qualifier to HBase column family and qualifier
    return new HBaseColumnName(kijiColumnName.getFamilyBytes(), kijiColumnName.getQualifierBytes());
  }

  /** {@inheritDoc} */
  @Override
  public byte[] toHBaseFamilyName(LocalityGroupLayout localityGroupLayout) {
    return Bytes.toBytes(localityGroupLayout.getName());
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout getTableLayout() {
    return mTableLayout;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("table", mTableLayout.getName())
        .add("columnNameTranslator", mTableLayout.getDesc().getColumnNameTranslator().toString())
        .toString();
  }
}
