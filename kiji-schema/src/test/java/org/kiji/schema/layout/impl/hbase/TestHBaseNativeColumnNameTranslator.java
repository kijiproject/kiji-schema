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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestHBaseNativeColumnNameTranslator {
  private HBaseColumnNameTranslator mTranslator;

  @Before
  public void readLayout() throws Exception {
    final KijiTableLayout tableLayout =
        KijiTableLayout.newLayout(
            KijiTableLayouts.getLayout(
                KijiTableLayouts.FULL_FEATURED_NATIVE));

    mTranslator = HBaseColumnNameTranslator.from(tableLayout);
  }

  @Test
  public void testTranslateFromKijiToHBase() throws Exception {
    HBaseColumnName infoName = mTranslator.toHBaseColumnName(KijiColumnName.create("info:name"));
    assertEquals("info", infoName.getFamilyAsString());
    assertEquals("name", infoName.getQualifierAsString());

    HBaseColumnName infoEmail = mTranslator.toHBaseColumnName(KijiColumnName.create("info:email"));
    assertEquals("info", infoEmail.getFamilyAsString());
    assertEquals("email", infoEmail.getQualifierAsString());

    HBaseColumnName recommendationsProduct = mTranslator.toHBaseColumnName(
        KijiColumnName.create("recommendations:product"));
    assertEquals("recommendations", recommendationsProduct.getFamilyAsString());
    assertEquals("product", recommendationsProduct.getQualifierAsString());

    HBaseColumnName purchases = mTranslator.toHBaseColumnName(
        KijiColumnName.create("recommendations:product"));
    assertEquals("recommendations", purchases.getFamilyAsString());
    assertEquals("product", purchases.getQualifierAsString());
  }

  @Test
  public void testTranslateFromHBaseToKiji() throws Exception {
    KijiColumnName infoName =
        mTranslator.toKijiColumnName(getHBaseColumnName("info", "name"));
    assertEquals("info:name", infoName.toString());

    KijiColumnName infoEmail =
        mTranslator.toKijiColumnName(getHBaseColumnName("info", "email"));
    assertEquals("info:email", infoEmail.toString());

    KijiColumnName recommendationsProduct = mTranslator.toKijiColumnName(
        getHBaseColumnName("recommendations", "product"));
    assertEquals("recommendations:product", recommendationsProduct.toString());

    KijiColumnName purchases =
        mTranslator.toKijiColumnName(getHBaseColumnName("recommendations", "product"));
    assertEquals("recommendations:product", purchases.toString());
  }

  /**
   * Tests that an exception is thrown when the HBase family doesn't match a Kiji locality group.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchKijiLocalityGroup() throws Exception {
    mTranslator.toKijiColumnName(getHBaseColumnName("fakeFamily", "fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when the second part of the HBase qualifier doesn't
   * match a Kiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchKijiColumn() throws Exception {
    mTranslator.toKijiColumnName(getHBaseColumnName("recommendations", "fakeQualifier"));
  }

  /**
   * Tests that an exception is thrown when trying to translate a non-existent Kiji column.
   */
  @Test(expected = NoSuchColumnException.class)
  public void testNoSuchHBaseColumn() throws Exception {
    mTranslator.toHBaseColumnName(KijiColumnName.create("doesnt:exist"));
  }

  /**
   * Tests that an exception is thrown when trying to instantiate a NativeKijiColumnTranslator
   * when the layout contains column families that don't match the locality group.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNativeLayout() throws Exception {
    KijiTableLayout invalidLayout =
        KijiTableLayout.newLayout(KijiTableLayouts.getLayout(KijiTableLayouts.INVALID_NATIVE));
    HBaseColumnNameTranslator.from(invalidLayout);
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
