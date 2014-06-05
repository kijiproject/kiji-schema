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

package org.kiji.schema.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.Filter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;

/**
 * The abstract base class for filters that exclude data from KijiRows.
 *
 * <p>Classes extending KijiRowFilter must implement the <code>hashCode</code> and
 * <code>equals</code> methods.</p>
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Extensible
public abstract class KijiRowFilter {
  /** The JSON node name used for the deserializer class. */
  private static final String DESERIALIZER_CLASS_NODE = "filterDeserializerClass";

  /** The JSON node name used for the filter fields. */
  private static final String FILTER_NODE = "filter";

  /**
   * Deserialize a {@code KijiRowFilter} from JSON that has been constructed
   * using {@link #toJson}.
   *
   * @param json A JSON String created by {@link #toJson}
   * @return A {@code KijiRowFilter} represented by the JSON
   * @throws KijiIOException in case the json cannot be read or the filter
   *     cannot be instantiated
   */
  public static KijiRowFilter toFilter(String json) {
    final ObjectMapper mapper = new ObjectMapper();
    try {
      final JsonNode root = mapper.readTree(json);
      return toFilter(root);
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  /**
   * Deserialize a {@code KijiRowFilter} from JSON that has been constructed
   * using {@link #toJson}.
   *
   * @param root A {@code JsonNode} created by {@link #toJson}
   * @return A {@code KijiRowFilter} represented by the JSON
   * @throws KijiIOException in case the filter cannot be instantiated
   */
  public static KijiRowFilter toFilter(JsonNode root) {
    final String filterDeserializerClassName = root.path(DESERIALIZER_CLASS_NODE).getTextValue();
    try {
      final Class<?> filterDeserializerClass = Class.forName(filterDeserializerClassName);
      final KijiRowFilterDeserializer filterDeserializer =
          (KijiRowFilterDeserializer) filterDeserializerClass.newInstance();
      final KijiRowFilter filter = filterDeserializer.createFromJson(root.path(FILTER_NODE));
      return filter;
    } catch (ClassNotFoundException cnfe) {
      throw new KijiIOException(cnfe);
    } catch (IllegalAccessException iae) {
      throw new KijiIOException(iae);
    } catch (InstantiationException ie) {
      throw new KijiIOException(ie);
    }
  }

  /**
   * A helper class for converting between Kiji objects and their HBase counterparts.
   */
  @ApiAudience.Public
  @Inheritance.Sealed
  public abstract static class Context {
    /**
     * Converts a Kiji row key into an HBase row key.
     *
     * <p>This method is useful only on tables with row key hashing disabled, since hashed
     * row keys have no ordering or semantics beyond being an identifier.</p>
     *
     * @param kijiRowKey A kiji row key.
     * @return The corresponding HBase row key.
     */
    public abstract byte[] getHBaseRowKey(String kijiRowKey);

    /**
     * Converts a Kiji column name to an HBase column family name.
     *
     * <p>Kiji optimizes cell storage in HBase by mapping user-provided column names to
     * compact identifiers (usually just a single byte). For example, what you refer to as
     * a column name "my_kiji_column" might actually be stored in HBase as a column name
     * with a single letter "B". Because of this, KijiRowFilter implementations that
     * reference Kiji column names should use this method to obtain the name of corresponding
     * optimized HBase column name.</p>
     *
     * @param kijiColumnName The name of a kiji column.
     * @return The name of the HBase column that stores the kiji column data.
     * @throws NoSuchColumnException If there is no such column in the kiji table.
     */
    public abstract HBaseColumnName getHBaseColumnName(KijiColumnName kijiColumnName)
        throws NoSuchColumnException;

    /**
     * Converts a Kiji cell value into an HBase cell value.
     *
     * <p>Kiji stores bytes into HBase cells by encoding an identifier for the writer schema
     * followed by the binary-encoded Avro data. Because of this, KijiRowFilter implementations
     * that inspect the contents of a cell should use this method to obtain the bytes encoded
     * as they would be stored by Kiji.</p>
     *
     * <p>For example, if you are implementing a KijiRowFilter that only considers rows
     * where column C contains an integer value of 123, construct an HBase Filter that
     * checks whether the contents of the cell in <code>getHBaseColumnName(C)</code> equals
     * <code>getHBaseCellValue(C, DecodedCell(<i>INT</i>, 123))</code>.</p>
     *
     * @param column Name of the column this cell belongs to.
     * @param kijiCell A kiji cell value.
     * @return The Kiji cell encoded as an HBase value.
     * @throws IOException If there is an error encoding the cell value.
     */
    public abstract byte[] getHBaseCellValue(KijiColumnName column, DecodedCell<?> kijiCell)
        throws IOException;
  }

  /**
   * Describes the data the filter requires to determine whether a row should be accepted.
   *
   * @return The data request.
   */
  public abstract KijiDataRequest getDataRequest();

  /**
   * Constructs an HBase <code>Filter</code> instance that can be used to instruct the
   * HBase region server which rows to filter.
   *
   * <p>You must use the given <code>context</code> object when referencing any HBase
   * table coordinates or values.  Using a Kiji row key, column family name, or column
   * qualifier name when configuring an HBase filter will result in undefined
   * behavior.</p>
   *
   * <p>For example, when constructing an HBase <code>SingleColumnValueFilter</code> to
   * inspect the "info:name" column of a Kiji table, use {@link
   * KijiRowFilter.Context#getHBaseColumnName(KijiColumnName)} to
   * retrieve the HBase column family and qualifier.  Here's an implementation that
   * filters out rows where the latest version of the 'info:name' is equal to 'Bob'.
   *
   * <pre>
   * KijiCell&lt;CharSequence&gt; bobCellValue = new KijiCell&lt;CharSequence&gt;(
   *     Schema.create(Schema.Type.STRING), "Bob");
   * HBaseColumnName hbaseColumn = context.getHBaseColumnName(
   *     KijiColumnName.create("info", "name"));
   * SingleColumnValueFilter filter = new SingleColumnValueFilter(
   *     hbaseColumn.getFamily(),
   *     hbaseColumn.getQualifier(),
   *     CompareOp.NOT_EQUAL,
   *     context.getHBaseCellValue(bobCellValue));
   * filter.setLatestVersionOnly(true);
   * filter.setFilterIfMissing(false);
   * return new SkipFilter(filter);</pre>
   * </p>
   *
   * @param context A helper object you can use to convert Kiji objects into their HBase
   *     counterparts.
   * @return The HBase filter the implements the semantics of this KijiRowFilter.
   * @throws IOException If there is an error.
   */
  public abstract Filter toHBaseFilter(Context context) throws IOException;

  /**
   * Constructs a {@code JsonNode} that describes the filter so that it may be
   * serialized.
   *
   * @return A {@code JsonNode} describing the filter
   */
  public final JsonNode toJson() {
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(DESERIALIZER_CLASS_NODE, getDeserializerClass().getName());
    root.put(FILTER_NODE, toJsonNode());
    return root;
  }

  /**
   * Constructs a {@code JsonNode} that holds the data structures specific to
   * the filter.  Implementing classes should include in the return node only
   * their own fields.
   *
   * @return A {@code JsonNode} containing the filter's fields
   */
  protected abstract JsonNode toJsonNode();

  /**
   * Returns {@code Class} that is responsible for deserializing the filter.
   * Subclasses that want to have non-default constructors with final fields can
   * define a different class to deserialize it; typically the deserializer
   * class will be a static inner class.
   *
   * @return a {@code KijiRowFilterDeserializer} class that will be responsible
   *     for deserializing this filter
   */
  protected abstract Class<? extends KijiRowFilterDeserializer> getDeserializerClass();
}
