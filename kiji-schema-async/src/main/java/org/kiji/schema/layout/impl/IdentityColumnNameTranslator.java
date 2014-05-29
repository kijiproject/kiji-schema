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
import org.apache.commons.lang.StringUtils;
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
 * families/qualifiers.  The identity mapping will use the Kiji locality group as the HBase
 * column family and the Kiji family:qualifier as the HBase qualifier.</p>
 *
 * <p>This class is not for the purpose of reading native HBase tables whose HBase
 * family/qualifiers are not in the localityGroup:family:qualifier format.</p>
 */
@ApiAudience.Private
public final class IdentityColumnNameTranslator extends KijiColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(IdentityColumnNameTranslator.class);

  /** Used to separate the Kiji family from the Kiji qualifier in an HBase qualifier. */
  public static final String SEPARATOR = ":";

  /** The table to translate names for. */
  private final KijiTableLayout mTableLayout;

  /**
   * Creates a new <code>IdentityColumnNameTranslator</code> instance.
   *
   * @param tableLayout The layout of the table to translate column names for.
   */
  public IdentityColumnNameTranslator(KijiTableLayout tableLayout) {
    mTableLayout = tableLayout;
  }

  /** {@inheritDoc} */
  @Override
  public KijiColumnName toKijiColumnName(HBaseColumnName hbaseColumnName)
      throws NoSuchColumnException {
    LOG.debug("Translating HBase column name '{}' to Kiji column name...", hbaseColumnName);
    final LocalityGroupLayout localityGroup =
        mTableLayout.getLocalityGroupMap().get(hbaseColumnName.getFamilyAsString());
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

    String family = parts[0];
    final FamilyLayout kijiFamily = mTableLayout.getFamilyMap().get(family);
    if (null == kijiFamily) {
      throw new NoSuchColumnException(String.format(
          "No family with ColumnId '%s' in locality group '%s'.",
          family, localityGroup.getDesc().getName()));
    }

    if (kijiFamily.isGroupType()) {
      // Group type family.
      String column = parts[1];
      final ColumnLayout kijiColumn = kijiFamily.getColumnMap().get(column);
      if (null == kijiColumn) {
        throw new NoSuchColumnException(String.format(
            "No column with ColumnId '%s' in family '%s'.",
            column, kijiFamily.getDesc().getName()));
      }
      final KijiColumnName result =
          new KijiColumnName(kijiFamily.getDesc().getName(), kijiColumn.getDesc().getName());
      LOG.debug("Translated to Kiji group column '{}'.", result);
      return result;
    }

    // Map type family.
    assert kijiFamily.isMapType();
    final KijiColumnName result = new KijiColumnName(kijiFamily.getDesc().getName(), parts[1]);
    LOG.debug("Translated to Kiji map column '{}'.", result);
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public HBaseColumnName toHBaseColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException {

    // Get the locality group that this Kiji column is in.  This is returned as part of the
    // HBase family.
    final String familyName = kijiColumnName.getFamily();
    final FamilyLayout fLayout = mTableLayout.getFamilyMap().get(familyName);
    if (null == fLayout) {
      throw new NoSuchColumnException(kijiColumnName.toString());
    }
    final String localityGroup = fLayout.getLocalityGroup().getName();

    return new HBaseColumnName(Bytes.toBytes(localityGroup), toHBaseQualifier(kijiColumnName));
  }

  /** {@inheritDoc} */
  @Override
  public byte[] toHBaseFamilyName(LocalityGroupLayout localityGroupLayout) {
    return Bytes.toBytes(localityGroupLayout.getName());
  }

  /**
   * Turns a KijiColumnName into an HBase qualifier.
   *
   * @param kijiColumnName The KijiColumnName to get the HBase qualifier for.
   * @return The HBase qualifier that should store this Kiji column's data.
   */
  private static byte[] toHBaseQualifier(KijiColumnName kijiColumnName) {
    StringBuilder hbaseQualifier = new StringBuilder()
        .append(kijiColumnName.getFamily())
        .append(SEPARATOR);
    if (null != kijiColumnName.getQualifier()) {
      hbaseQualifier.append(kijiColumnName.getQualifier());
    }
    return Bytes.toBytes(hbaseQualifier.toString());
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
