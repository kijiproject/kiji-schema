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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.util.FromJson;
import org.kiji.schema.util.ToJson;

/**
 * A utility class providing several table layouts for testing.
 */
public final class KijiTableLayouts {
  /**
   * Loads a table layout descriptor from a JSON resource.
   *
   * @param resourcePath Path of the resource to load the JSON descriptor from.
   * @return the decoded TableLayoutDesc.
   * @throws IOException on I/O error.
   */
  public static TableLayoutDesc getLayout(String resourcePath) throws IOException {
    final InputStream istream =
        KijiTableLayouts.class.getClassLoader().getResourceAsStream(resourcePath);
    try {
      final String json = IOUtils.toString(istream);
      return (TableLayoutDesc) FromJson.fromJsonString(json, TableLayoutDesc.SCHEMA$);
    } finally {
      istream.close();
    }
  }

  /** A fully-featured layout example. */
  public static final String FULL_FEATURED = "org/kiji/schema/layout/full-featured-layout.json";

  /** Fully-featured layout example with inline and class schemas. */
  public static final String FULL_FEATURED_INLINE_SCHEMA =
      "org/kiji/schema/layout/full-featured-layout-inline-schemas.json";

  /** A simple layout for a user table. */
  public static final String USER_TABLE =
      "org/kiji/schema/layout/user-table.json";

  /** A simple layout file example (bare minimum). */
  public static final String SIMPLE =
      "org/kiji/schema/layout/simple.json";

  /** A simple unhashed layout file example (bare minimum). */
  public static final String SIMPLE_UNHASHED =
      "org/kiji/schema/layout/simple-unhashed.json";

  /** Simple layout example with an extra int column called 'new'. */
  public static final String SIMPLE_UPDATE_NEW_COLUMN =
      "org/kiji/schema/layout/simple-update-new-column.json";

  /** A simple layout file example with an extra locality group called 'new'. */
  public static final String SIMPLE_UPDATE_NEW_LOCALITY_GROUP =
      "org/kiji/schema/layout/simple-update-new-locality-group.json";

  /** A simple layout file example with two columns. */
  public static final String SIMPLE_TWO_COLUMNS =
      "org/kiji/schema/layout/simple-two-columns.json";

  /** A layout file which uses several primitive types as schemas for the columns. */
  public static final String PRIMITIVE_TYPES =
      "org/kiji/schema/layout/primitive-types.json";

  /** A layout file with counter schemas. */
  public static final String COUNTER_TEST =
      "org/kiji/schema/layout/counter-test.json";

  /** Test layout for paging. */
  public static final String PAGING_TEST =
      "org/kiji/schema/layout/paging-test.json";

  public static final String ROW_DATA_TEST =
      "org/kiji/schema/layout/row-data-test.json";

  /** Layout to test sqoop export map. */
  public static final String SQOOP_EXPORT_MAP_TEST =
      "org/kiji/schema/layout/sqoop-export-map-test.json";

  /** Layout to test sqoop export sampling with row key hashing enabled. */
  public static final String SQOOP_EXPORT_SAMPLING_HASHED_TEST =
      "org/kiji/schema/layout/sqoop-export-sampling-hashed-test.json";

  /** Layout to test sqoop export sampling with row key hashing disabled. */
  public static final String SQOOP_EXPORT_SAMPLING_UNHASHED_TEST =
      "org/kiji/schema/layout/sqoop-export-sampling-unhashed-test.json";

  /** Layout to test sqoop export with varying types. */
  public static final String SQOOP_EXPORT_VARYING_TYPED_TEST =
      "org/kiji/schema/layout/sqoop-export-varying-typed-test.json";

  /** Test layout named 'foo'. */
  public static final String FOO_TEST =
      "org/kiji/schema/layout/foo-test.json";

  /** Table named 'table' with a final string column named 'family:column'. */
  public static final String FINAL_COLUMN =
      "org/kiji/schema/layout/final-column.json";

  /** Test layout with hashing disabled. */
  public static TableLayoutDesc getFooUnhashedTestLayout() throws IOException {
    final TableLayoutDesc desc = getLayout(FOO_TEST);
    desc.setName("foo_nonhashed");
    desc.getKeysFormat().setEncoding(RowKeyEncoding.RAW);
    return desc;
  }

  /** Test changing the row key hashing property. */
  public static TableLayoutDesc getFooChangeHashingTestLayout() throws IOException {
    final TableLayoutDesc desc = getLayout(FOO_TEST);
    desc.getKeysFormat().setEncoding(RowKeyEncoding.RAW);
    desc.setReferenceLayout("1");
    return desc;
  }

  /**
   * Layout to test simple kmeans.
   * The 'simple_kmeans_data' table has a single column family 'info' with two columns in it:
   * <ul>
   *   <li>username - The user's name.</li>
   *   <li>height - The height of a person</li>
   *   <li>weight - The weight of this person</li>
   * </ul>
   */
  public static final String SIMPLE_KMEANS_TEST =
      "org/kiji/schema/layout/simple-kmeans-test.json";

  /** Layout to test deletes. */
  public static final String DELETES_TEST =
      "org/kiji/schema/layout/deletes-test.json";

  public static final String GATHER_MAP_TEST =
      "org/kiji/schema/layout/gather-map-test.json";

  public static final String FOODS =
      "org/kiji/schema/layout/foods.json";

  /** Layout to test regex filters. */
  public static final String REGEX =
      "org/kiji/schema/layout/regex.json";

  /** Layout with invalid schema. */
  public static final String INVALID_SCHEMA =
      "org/kiji/schema/layout/invalid-schema.json";

  /** Layout to test bulk loads during setup and cleanup methods. */
  public static final String SETUP_CLEANUP_TABLE_TEST =
      "org/kiji/schema/layout/setup-cleanup-table-test.json";

  /** Layout to test the identity producer. */
  public static final String IDENTITY_PRODUCER_TEST =
      "org/kiji/schema/layout/identity-producer-test.json";

  /**
   * Creates a temporary JSON file with the specified layout.
   *
   * @param desc Layout descriptor.
   * @return Temporary JSON file containing the specified layout.
   * @throws IOException on I/O error.
   */
  public static File getTempFile(TableLayoutDesc desc) throws IOException {
    final File layoutFile = File.createTempFile("layout-" + desc.getName(), "json");
    layoutFile.deleteOnExit();
    final OutputStream fos = new FileOutputStream(layoutFile);
    IOUtils.write(ToJson.toJsonString(desc), fos);
    fos.close();
    return layoutFile;
  }

  /** Disable constructor for this utility class. */
  private KijiTableLayouts() {}
}
