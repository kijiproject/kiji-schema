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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;

public class TestHBaseNativeColumnNameTranslator {
  private KijiTableLayout mTableLayout;

  @Before
  public void readLayout() throws Exception {
    mTableLayout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(
            KijiTableLayouts.FULL_FEATURED_NATIVE));
  }

  @Test
  public void testTranslateFromKijiToHBase() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    HBaseColumnName infoName = translator.toHBaseColumnName(new KijiColumnName("info:name"));
    assertEquals("info", infoName.getFamilyAsString());
    assertEquals("name", infoName.getQualifierAsString());

    HBaseColumnName infoEmail = translator.toHBaseColumnName(new KijiColumnName("info:email"));
    assertEquals("info", infoEmail.getFamilyAsString());
    assertEquals("email", infoEmail.getQualifierAsString());

    HBaseColumnName recommendationsProduct = translator.toHBaseColumnName(
        new KijiColumnName("recommendations:product"));
    assertEquals("recommendations", recommendationsProduct.getFamilyAsString());
    assertEquals("product", recommendationsProduct.getQualifierAsString());

    HBaseColumnName purchases = translator.toHBaseColumnName(
        new KijiColumnName("recommendations:product"));
    assertEquals("recommendations", purchases.getFamilyAsString());
    assertEquals("product", purchases.getQualifierAsString());
  }

  @Test
  public void testTranslateFromHBaseToKiji() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    KijiColumnName infoName =
        translator.toKijiColumnName(getHBaseColumnName("info", "name"));
    assertEquals("info:name", infoName.toString());

    KijiColumnName infoEmail =
        translator.toKijiColumnName(getHBaseColumnName("info", "email"));
    assertEquals("info:email", infoEmail.toString());

    KijiColumnName recommendationsProduct = translator.toKijiColumnName(
        getHBaseColumnName("recommendations", "product"));
    assertEquals("recommendations:product", recommendationsProduct.toString());

    KijiColumnName purchases =
        translator.toKijiColumnName(getHBaseColumnName("recommendations", "product"));
    assertEquals("recommendations:product", purchases.toString());
  }

  /**
   * Tests that an exception is thrown when the HBase family doesn't match a Kiji locality group.
   */
  @Test
  public void testNoSuchKijiLocalityGroup() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    try {
      translator.toKijiColumnName(
          getHBaseColumnName("fakeFamily", "fakeQualifier"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("No family 'fakeFamily' in layout.",
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
      translator.toKijiColumnName(getHBaseColumnName("recommendations", "fakeQualifier"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("No column with ColumnId 'fakeQualifier' in family 'recommendations'.",
          nsce.getMessage());
    }
  }

  /**
   * Tests that an exception is thrown when trying to translate a non-existent Kiji column.
   */
  @Test
  public void testNoSuchHBaseColumn() throws Exception {
    KijiColumnNameTranslator translator = KijiColumnNameTranslator.from(mTableLayout);

    try {
      translator.toHBaseColumnName(new KijiColumnName("doesnt:exist"));
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("No family 'doesnt' in layout.", nsce.getMessage());
    }
  }

  /**
   * Tests that an exception is thrown when trying to instantiate a NativeKijiColumnTranslator
   * when the layout contains column families that don't match the locality group.
   */
  @Test
  public void testInvalidNativeLayout() throws Exception {
    try {
      KijiTableLayout invalidLayout =
          KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.INVALID_NATIVE));
      KijiColumnNameTranslator.from(invalidLayout);
      fail("An exception should have been thrown");
    } catch (IllegalStateException ise) {
      assertTrue(ise.getMessage().startsWith("For HBASE_NATIVE column name translation"));
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
