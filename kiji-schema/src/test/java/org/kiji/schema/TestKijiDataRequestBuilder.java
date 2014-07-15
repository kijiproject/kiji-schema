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

package org.kiji.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;

public class TestKijiDataRequestBuilder {
  @Test
  public void testBuild() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "foo");
    KijiDataRequest request = builder.build();

    // We should be able to use KijiDataRequest's create() method to similar effect.
    KijiDataRequest request2 = KijiDataRequest.create("info", "foo");
    assertEquals("Constructions methods make different requests", request, request2);
    assertTrue("Builder doesn't build a new object", request != request2);

    assertNotNull("Missing info:foo!", request.getColumn("info", "foo"));
    assertNull("Got spurious info:missing", request.getColumn("info", "missing"));
    assertNull("Got spurious info: family", request.getColumn("info", null));
  }

  @Test
  public void testBuildMore() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "foo");
    builder.newColumnsDef().add("info", "bar");
    builder.newColumnsDef().addFamily("map");
    KijiDataRequest request = builder.build();

    // Reuse the column builder.
    KijiDataRequestBuilder builder2 = KijiDataRequest.builder();
    builder2.newColumnsDef().add("info", "foo").add("info", "bar").addFamily("map");
    KijiDataRequest request2 = builder2.build();

    // These should have the same effect.
    assertEquals(request, request2);

    KijiDataRequestBuilder builder3 = KijiDataRequest.builder();
    builder3.withTimeRange(3, 4);
    builder3.newColumnsDef().withMaxVersions(10).add("info", "foo");
    builder3.newColumnsDef().withPageSize(6).add("info", "bar");
    KijiDataRequest request3 = builder3.build();

    assertNotNull("missing the expected column", request3.getColumn("info", "foo"));
    KijiDataRequest.Column foo3 = request3.getColumn("info", "foo");
    KijiDataRequest.Column bar3 = request3.getColumn("info", "bar");
    assertEquals("Wrong maxVersions for info:foo", 10, foo3.getMaxVersions());
    assertEquals("Wrong pageSize for info:foo", 0, foo3.getPageSize());
    assertEquals("Wrong maxVersions for info:bar", 1, bar3.getMaxVersions());
    assertEquals("Wrong pageSize for info:bar", 6, bar3.getPageSize());
    assertEquals("Wrong minTs", 3, request3.getMinTimestamp());
    assertEquals("Wrong maxTs", 4, request3.getMaxTimestamp());
  }

  @Test
  public void testBuildMapFamiliesTwoWays() {
    KijiDataRequestBuilder builder1 = KijiDataRequest.builder();
    builder1.newColumnsDef().addFamily("info");
    KijiDataRequest req1 = builder1.build();

    KijiDataRequestBuilder builder2 = KijiDataRequest.builder();
    builder2.newColumnsDef().add("info", null);
    KijiDataRequest req2 = builder2.build();

    assertEquals("These are equivalent ways of specifying a KDR, with unequal results",
        req1, req2);
  }

  @Test
  public void testNoRedundantColumn() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    try {
      builder.newColumnsDef().add("info", "foo").add("info", "foo");
      fail("Should have thrown exception for redundant add");
    } catch (IllegalArgumentException ise) {
      assertEquals("Duplicate request for column 'info:foo'.", ise.getMessage());
    }
  }

  @Test
  public void testNoRedundantColumnIn2ColBuilders() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "foo");
    builder.newColumnsDef().add("info", "foo");
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("Duplicate definition for column 'Column{name=info:foo, max_versions=1}'.",
          ise.getMessage());
    }
  }

  @Test
  public void testNoRedundantColumnWithFamily() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "foo").addFamily("info");
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("KijiDataRequest may not simultaneously contain definitions for family 'info' "
          + "and definitions for fully qualified columns in family 'info'.", ise.getMessage());
    }
  }

  @Test
  public void testNoRedundantColumnWithFamilyIn2ColBuilders() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "foo");
    builder.newColumnsDef().addFamily("info");
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("KijiDataRequest may not simultaneously contain definitions for family 'info' "
          + "and definitions for fully qualified columns in family 'info'.", ise.getMessage());
    }
  }

  @Test
  public void testNoRedundantColumnWithFamilyReversed() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().addFamily("info").add("info", "foo");
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("KijiDataRequest may not simultaneously contain definitions for family 'info' "
          + "and definitions for fully qualified columns 'info:foo'.", ise.getMessage());
    }
  }

  @Test
  public void testNoRedundantColumnWithFamilyIn2ColBuildersReversed() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().addFamily("info");
    builder.newColumnsDef().add("info", "foo");
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("KijiDataRequest may not simultaneously contain definitions for family 'info' "
          + "and definitions for fully qualified columns 'info:foo'.", ise.getMessage());
    }
  }

  @Test
  public void testNoNegativeMaxVer() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    try {
      builder.newColumnsDef().withMaxVersions(-5).addFamily("info");
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Maximum number of versions must be strictly positive, but got: -5",
          iae.getMessage());
    }
  }

  @Test
  public void testNoNegativePageSize() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    try {
      builder.newColumnsDef().withPageSize(-5).addFamily("info");
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Page size must be 0 (disabled) or positive, but got: null",
          iae.getMessage());
    }
  }

  @Test
  public void testNoZeroMaxVersions() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    try {
      builder.newColumnsDef().withMaxVersions(0).addFamily("info");
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Maximum number of versions must be strictly positive, but got: 0",
          iae.getMessage());
    }
  }

  @Test
  public void testZeroPageSizeOk() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withPageSize(0).addFamily("info");
    builder.build();
  }

  @Test
  public void testEmptyOk() {
    assertNotNull(KijiDataRequest.builder().build());
  }

  @Test
  public void testNoSettingMaxVerTwice() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    try {
      builder.newColumnsDef().withMaxVersions(2).withMaxVersions(3).addFamily("info");
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot set max versions to 3, max versions already set to 2.",
          ise.getMessage());
    }
  }

  @Test
  public void testNoSettingPageSizeTwice() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    try {
      builder.newColumnsDef().withPageSize(2).withPageSize(3).addFamily("info");
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot set page size to 3, page size already set to 2.",
          ise.getMessage());
    }
  }

  @Test
  public void testNoSettingTimeRangeTwice() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    try {
      builder.withTimeRange(2, 3).withTimeRange(6, 9);
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot set time range more than once.", ise.getMessage());
    }
  }

  @Test
  public void testNoPropertiesAfterAdd() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    try {
      builder.newColumnsDef().add("info", "foo").withMaxVersions(5);
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals(
          "Properties of the columns builder cannot be changed once columns are assigned to it.",
          ise.getMessage());
    }
  }

  @Test
  public void testNoAddAfterBuild() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(5).add("info", "foo");
    builder.build();
    try {
      builder.newColumnsDef().add("info", "bar");
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("KijiDataRequest builder cannot be used after build() is invoked.",
          ise.getMessage());
    }
  }

  @Test
  public void testNoPropertiesAfterBuild() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef columns = builder.newColumnsDef();
    columns.withMaxVersions(5).add("info", "foo");
    builder.build();
    try {
      columns.add("info", "bar");
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot add more columns to this ColumnsDef after build() has been called.",
          ise.getMessage());
    }
  }

  @Test
  public void testCannotBuildTwoTimes() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef columns = builder.newColumnsDef();
    columns.withMaxVersions(5).add("info", "foo");
    builder.build();
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("KijiDataRequest builder cannot be used after build() is invoked.",
          ise.getMessage());
    }
  }

  @Test
  public void testChaining() {
    final KijiDataRequest kdr = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(10).addFamily("info"))
        .addColumns(ColumnsDef.create().add("users", "details"))
        .build();
    assertEquals(2, kdr.getColumns().size());
    final KijiDataRequest.Column infoCol = kdr.getColumn("info", null);
    assertEquals("info", infoCol.getFamily());
    assertEquals(null, infoCol.getQualifier());
    final KijiDataRequest.Column usersDetailsCol = kdr.getColumn("users", "details");
    assertEquals("users", usersDetailsCol.getFamily());
    assertEquals("details", usersDetailsCol.getQualifier());
  }

  @Test
  public void testNoColumnsDefReuse() {
    final ColumnsDef cols = ColumnsDef.create().withMaxVersions(10).addFamily("info");
    KijiDataRequest.builder().addColumns(cols).build();  // seals cols
    try {
      KijiDataRequest.builder().addColumns(cols).build();  // fails as cols is already sealed.
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("ColumnsDef is sealed.  This usually indicates that the ColumnsDef has been "
          + "used in the construction of another KijiDataRequest already.", ise.getMessage());
    }
  }
}
