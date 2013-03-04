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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.impl.ColumnNameTranslator;


public class TestColumnNameTranslator {
  private KijiTableLayout mTableLayout;

  @Before
  public void readLayout() throws Exception {
    mTableLayout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED));
    System.out.println(mTableLayout);
  }

  @Test
  public void testTranslateFromKijiToHBase() throws Exception {
    ColumnNameTranslator translator = new ColumnNameTranslator(mTableLayout);

    HBaseColumnName infoName = translator.toHBaseColumnName(new KijiColumnName("info:name"));
    assertEquals("B", infoName.getFamilyAsString());
    assertEquals("B:B", infoName.getQualifierAsString());

    HBaseColumnName infoEmail = translator.toHBaseColumnName(new KijiColumnName("info:email"));
    assertEquals("B", infoEmail.getFamilyAsString());
    assertEquals("B:C", infoEmail.getQualifierAsString());

    HBaseColumnName recommendationsProduct = translator.toHBaseColumnName(
        new KijiColumnName("recommendations:product"));
    assertEquals("C", recommendationsProduct.getFamilyAsString());
    assertEquals("B:B", recommendationsProduct.getQualifierAsString());

    HBaseColumnName purchases = translator.toHBaseColumnName(new KijiColumnName("purchases:foo"));
    assertEquals("C", purchases.getFamilyAsString());
    assertEquals("C:foo", purchases.getQualifierAsString());
  }

  @Test
  public void testTranslateFromHBaseToKiji() throws Exception {
    ColumnNameTranslator translator = new ColumnNameTranslator(mTableLayout);

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
  @Test(expected=NoSuchColumnException.class)
  public void testNoSuchKijiLocalityGroup() throws Exception {
    ColumnNameTranslator translator = new ColumnNameTranslator(mTableLayout);

    translator.toKijiColumnName(getHBaseColumnName("D", "E:E"));
  }

  /**
   * Tests that an exception is thrown when the first part of the HBase qualifier doesn't
   * match a Kiji family.
   */
  @Test(expected=NoSuchColumnException.class)
  public void testNoSuchKijiFamily() throws Exception {
    ColumnNameTranslator translator = new ColumnNameTranslator(mTableLayout);

    translator.toKijiColumnName(getHBaseColumnName("B", "E:E"));
  }

  /**
   * Tests that an exception is thrown when the second part of the HBase qualifier doesn't
   * match a Kiji column.
   */
  @Test(expected=NoSuchColumnException.class)
  public void testNoSuchKijiColumn() throws Exception {
    ColumnNameTranslator translator = new ColumnNameTranslator(mTableLayout);

    translator.toKijiColumnName(getHBaseColumnName("C", "B:E"));
  }

  /**
   * Tests that an exception is thrown when the HBase qualifier is corrupt (no separator).
   */
  @Test(expected=NoSuchColumnException.class)
  public void testCorruptQualifier() throws Exception {
    ColumnNameTranslator translator = new ColumnNameTranslator(mTableLayout);

    translator.toKijiColumnName(getHBaseColumnName("C", "BE"));
  }

  /**
   * Tests that an exception is thrown when trying to translate a non-existed Kiji column.
   */
  @Test(expected=NoSuchColumnException.class)
  public void testNoSuchHBaseColumn() throws Exception {
    ColumnNameTranslator translator = new ColumnNameTranslator(mTableLayout);

    translator.toHBaseColumnName(new KijiColumnName("doesnt:exist"));
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
