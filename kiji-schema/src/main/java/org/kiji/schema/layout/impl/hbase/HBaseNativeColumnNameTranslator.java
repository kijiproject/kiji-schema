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

package org.kiji.schema.layout.impl.hbase;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

/**
 * Translates between HBase and Kiji column names.
 *
 * <p>This class defines a mapping between names of Kiji locality group/family/qualifiers and HBase
 * families/qualifiers.  The native mapping will use the Kiji family as the
 * HBase column family and the Kiji qualifier as the HBase qualifier.  Kiji locality groups
 * MUST be specified to be the same as the Kiji column family.</p>
 *
 * <p>This class is for the purpose of reading native HBase tables whose HBase
 * family/qualifiers do not use the notion of locality groups.</p>
 */
@ApiAudience.Private
public final class HBaseNativeColumnNameTranslator extends HBaseColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseNativeColumnNameTranslator.class);

  /** The table to translate names for. */
  private final KijiTableLayout mLayout;

  /**
   * Creates a new {@link HBaseNativeColumnNameTranslator} instance.
   *
   * @param layout The layout of the table to translate column names for.
   */
  public HBaseNativeColumnNameTranslator(KijiTableLayout layout) {
    mLayout = layout;

    for (FamilyLayout family : mLayout.getFamilies()) {
      // Validate that all Kiji column families are the same as their respective locality groups
      Preconditions.checkArgument(family.getName().equals(family.getLocalityGroup().getName()),
          "For HBASE_NATIVE column name translation, family: '%s' must match locality group: '%s'",
          family.getName(), family.getLocalityGroup().getName());

      // Validate all Kiji column families are group type
      Preconditions.checkArgument(family.isGroupType(),
          "For HBASE_NATIVE column name translation, family: '%s' must be a group type.",
          family.getName());
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiColumnName toKijiColumnName(HBaseColumnName hbaseColumnName)
      throws NoSuchColumnException {
    LOG.debug("Translating HBase column name '{}' to Kiji column name.", hbaseColumnName);

    final String familyName = Bytes.toString(hbaseColumnName.getFamily());
    final String qualifierName = Bytes.toString(hbaseColumnName.getQualifier());

    final FamilyLayout family = mLayout.getFamilyMap().get(familyName);

    // Validate that the family exists
    if (family == null) {
      throw new NoSuchColumnException(String.format("No family %s in layout for table %s.",
          familyName, mLayout.getName()));
    }

    // Validate that the qualifier exists
    final ColumnLayout qualifier = family.getColumnMap().get(qualifierName);
    if (qualifier == null) {
      throw new NoSuchColumnException(String.format("No qualifier %s in family %s of table %s.",
          qualifierName, familyName, mLayout.getName()));
    }

    final KijiColumnName kijiColumnName = new KijiColumnName(familyName, qualifierName);

    LOG.debug("Translated to Kiji column '{}'.", kijiColumnName);
    return kijiColumnName;
  }

  /** {@inheritDoc} */
  @Override
  public HBaseColumnName toHBaseColumnName(KijiColumnName kijiColumnName)
      throws NoSuchColumnException {
    final String familyName = kijiColumnName.getFamily();
    final String qualifierName = kijiColumnName.getQualifier();

    // Validate that the family exists
    final FamilyLayout family = mLayout.getFamilyMap().get(familyName);
    if (family == null) {
      throw new NoSuchColumnException(String.format("No family %s in table %s.",
          familyName, mLayout.getName()));
    }

    // Validate that the qualifier exists within the layout
    if (!family.getColumnMap().containsKey(qualifierName)) {
      throw new NoSuchColumnException(String.format(
          "No qualifier %s in family %s of table %s.",
          qualifierName, familyName, mLayout.getName()));
    }

    return new HBaseColumnName(
        Bytes.toBytes(kijiColumnName.getFamily()),
        Bytes.toBytes(kijiColumnName.getQualifier()));
  }

  /** {@inheritDoc} */
  @Override
  public byte[] toHBaseFamilyName(LocalityGroupLayout localityGroup) {
    return Bytes.toBytes(localityGroup.getName());
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout getTableLayout() {
    return mLayout;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("table", mLayout.getName())
        .add("layout", mLayout)
        .toString();
  }
}
