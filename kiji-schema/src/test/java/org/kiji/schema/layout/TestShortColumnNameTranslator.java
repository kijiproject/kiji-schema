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

public class TestShortColumnNameTranslator {
  private KijiTableLayout mTableLayout;

  @Before
  public void readLayout() throws Exception {
    mTableLayout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED));
    System.out.println(mTableLayout);
  }

  @Test
  public void testTranslateFromKijiToHBase() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    HBaseColumnName infoName = translator.toHBaseColumnName(KijiColumnName.create("info:name"));
    assertEquals("B", infoName.getFamilyAsString());
    assertEquals("B:B", infoName.getQualifierAsString());

    HBaseColumnName infoEmail = translator.toHBaseColumnName(KijiColumnName.create("info:email"));
    assertEquals("B", infoEmail.getFamilyAsString());
    assertEquals("B:C", infoEmail.getQualifierAsString());

    HBaseColumnName recommendationsProduct = translator.toHBaseColumnName(
        KijiColumnName.create("recommendations:product"));
    assertEquals("C", recommendationsProduct.getFamilyAsString());
    assertEquals("B:B", recommendationsProduct.getQualifierAsString());

    HBaseColumnName purchases = translator.toHBaseColumnName(
        KijiColumnName.create("purchases:foo"));
    assertEquals("C", purchases.getFamilyAsString());
    assertEquals("C:foo", purchases.getQualifierAsString());
  }

  @Test
  public void testTranslateFromHBaseToKiji() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    KijiColumnName infoName = translator.toKijiColumnName(getHBaseColumnName("B", "B:B"));
    assertEquals("info:name", infoName.toString());

    KijiColumnName infoEmail = translator.toKijiColumnName(getHBaseColumnName("B", "B:C"));
    assertEquals("info:email", infoEmail.toString());

    KijiColumnName recommendationsProduct = translator.toKijiColumnName(
        getHBaseColumnName("C", "B:B"));
    assertEquals("recommendations:product", recommendationsProduct.toString());

    KijiColumnName purchases = translator.toKijiColumnName(getHBaseColumnName("C", "C:foo"));
    assertEquals("purchases:foo", purchases.toString());
  }

  /**
   * Tests that an exception is thrown when the HBase family doesn't match a Kiji locality group.
   */
  @Test
  public void testNoSuchKijiLocalityGroup() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    try {
      translator.toKijiColumnName(getHBaseColumnName("D", "E:E"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("No locality group with ID/HBase family: 'D'.", nsce.getMessage());
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
      translator.toKijiColumnName(getHBaseColumnName("C", "E:E"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("No family with ColumnId 'E' in locality group 'inMemory'.", nsce.getMessage());
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
      translator.toKijiColumnName(getHBaseColumnName("C", "B:E"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("No column with ColumnId 'E' in family 'recommendations'.", nsce.getMessage());
    }
  }

  /**
   * Tests that an exception is thrown when the HBase qualifier is corrupt (no separator).
   */
  @Test
  public void testCorruptQualifier() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    try {
      translator.toKijiColumnName(getHBaseColumnName("C", "BE"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Missing separator (:) from HBase qualifier (BE). Unable to parse Kiji "
          + "family/qualifier pair.", nsce.getMessage());
    }
  }

  /**
   * Tests that an exception is thrown when trying to translate a non-existed Kiji column.
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
