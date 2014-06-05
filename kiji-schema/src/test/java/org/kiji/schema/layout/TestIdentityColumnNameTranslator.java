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

package org.kiji.schema.layout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;

public class TestIdentityColumnNameTranslator {
  private KijiTableLayout mTableLayout;

  @Before
  public void readLayout() throws Exception {
    mTableLayout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(
            KijiTableLayouts.FULL_FEATURED_IDENTITY));
  }

  @Test
  public void testTranslateFromKijiToHBase() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    HBaseColumnName infoName = translator.toHBaseColumnName(KijiColumnName.create("info:name"));
    assertEquals("default", infoName.getFamilyAsString());
    assertEquals("info:name", infoName.getQualifierAsString());

    HBaseColumnName infoEmail = translator.toHBaseColumnName(KijiColumnName.create("info:email"));
    assertEquals("default", infoEmail.getFamilyAsString());
    assertEquals("info:email", infoEmail.getQualifierAsString());

    HBaseColumnName recommendationsProduct = translator.toHBaseColumnName(
        KijiColumnName.create("recommendations:product"));
    assertEquals("inMemory", recommendationsProduct.getFamilyAsString());
    assertEquals("recommendations:product", recommendationsProduct.getQualifierAsString());

    HBaseColumnName purchases = translator.toHBaseColumnName(
        KijiColumnName.create("purchases:foo"));
    assertEquals("inMemory", purchases.getFamilyAsString());
    assertEquals("purchases:foo", purchases.getQualifierAsString());
  }

  @Test
  public void testTranslateFromHBaseToKiji() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    KijiColumnName infoName =
        translator.toKijiColumnName(getHBaseColumnName("default", "info:name"));
    assertEquals("info:name", infoName.toString());

    KijiColumnName infoEmail =
        translator.toKijiColumnName(getHBaseColumnName("default", "info:email"));
    assertEquals("info:email", infoEmail.toString());

    KijiColumnName recommendationsProduct = translator.toKijiColumnName(
        getHBaseColumnName("inMemory", "recommendations:product"));
    assertEquals("recommendations:product", recommendationsProduct.toString());

    KijiColumnName purchases =
        translator.toKijiColumnName(getHBaseColumnName("inMemory", "purchases:foo"));
    assertEquals("purchases:foo", purchases.toString());
  }

  /**
   * Tests that an exception is thrown when the HBase family doesn't match a Kiji locality group.
   */
  @Test
  public void testNoSuchKijiLocalityGroup() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    try {
      translator.toKijiColumnName(
          getHBaseColumnName("fakeLocalityGroup", "fakeFamily:fakeQualifier"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("No locality group with ID/HBase family: 'fakeLocalityGroup'.",
          nsce.getMessage());
    }
  }

  /**
   * Tests that an exception is thrown when the first part of the HBase qualifier doesn't
   * match a Kiji family.
   */
  @Test
  public void testNoSuchKijiFamily() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
    try {
      translator.toKijiColumnName(getHBaseColumnName("inMemory", "fakeFamily:fakeQualifier"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("No family with ColumnId 'fakeFamily' in locality group 'inMemory'.",
          nsce.getMessage());
    }
  }

  /**
   * Tests that an exception is thrown when the second part of the HBase qualifier doesn't
   * match a Kiji column.
   */
  @Test
  public void testNoSuchKijiColumn() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    try {
      translator.toKijiColumnName(getHBaseColumnName("inMemory", "recommendations:fakeQualifier"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("No column with ColumnId 'fakeQualifier' in family 'recommendations'.",
          nsce.getMessage());
    }
  }

  /**
   * Tests that an exception is thrown when the HBase qualifier is corrupt (no separator).
   */
  @Test
  public void testCorruptQualifier() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    try {
      translator.toKijiColumnName(getHBaseColumnName("inMemory", "fakeFamilyfakeQualifier"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Missing separator (:) from HBase qualifier (fakeFamilyfakeQualifier). "
          + "Unable to parse Kiji family/qualifier pair.", nsce.getMessage());
    }
  }

  /**
   * Tests translation of HBase qualifiers have multiple separators (the Kiji qualifier contains
   * the separator).
   */
  @Test
  public void testMultipleSeparators() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);
      KijiColumnName kijiColumnName =
          translator.toKijiColumnName(getHBaseColumnName("inMemory", "purchases:left:right"));
    assertEquals("purchases", kijiColumnName.getFamily());
    assertEquals("left:right", kijiColumnName.getQualifier());
  }

  /**
   * Tests that an exception is thrown when trying to translate a non-existent Kiji column.
   */
  @Test
  public void testNoSuchHBaseColumn() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    try {
      translator.toHBaseColumnName(KijiColumnName.create("doesnt:exist"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("doesnt:exist", nsce.getMessage());
    }
  }

  /**
   * Turns a family:qualifier string into an HBaseColumnName.
   *
   * @param family The HBase family.
   * @param qualifier the HBase qualifier.
   * @return An HBaseColumnName instance.
   */
  private static HBaseColumnName getHBaseColumnName(String family, String qualifier) {
    return new HBaseColumnName(Bytes.toBytes(family), Bytes.toBytes(qualifier));
  }
}
