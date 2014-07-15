/**
 * (c) Copyright 2013 WibiData, Inc.
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

import java.io.IOException;
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.layout.CellSpec;

/**
 * Interface for table reader factories.
 *
 * <p> Use <code>KijiTable.getReaderFactory()</code> to get a reader.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface KijiReaderFactory {

  /**
   * Get the table from which readers built by this factory will read.
   *
   * @return the table from which readers built by this factory will read.
   */
  KijiTable getTable();

  /**
   * Opens a new reader for the KijiTable associated with this reader factory.
   *
   * <p>
   *   This method is equivalent of <code>readerBuilder().build()</code>. It sets all options to
   *   their default values.
   * </p>
   *
   * <p> The caller of this method is responsible for closing the reader. </p>
   * <p>
   *   The reader returned by this method does not provide any isolation guarantee.
   *   In particular, you should assume that the underlying resources (connections, buffers, etc)
   *   are used concurrently for other purposes.
   * </p>
   *
   * @return a new KijiTableReader.
   * @throws IOException on I/O error.
   */
  KijiTableReader openTableReader() throws IOException;

  /**
   * Opens a new reader for the KijiTable associated with this reader factory.
   *
   * <p>
   *   This factory method lets the user customize the layout of the table.
   *   In particular, for each column, the user may:
   *   <ul>
   *     <li> choose between generic and specific Avro records. </li>
   *     <li> specify different Avro reader schemas. </li>
   *     <li> request the Avro writer schemas (this forces using generic records). </li>
   *   </ul>
   * </p>
   *
   * <p>
   *   By default, the reader attempts to use Avro specific records if they are available
   *   on the classpath, and falls back to using generic records if a specific record
   *   is not available on the classpath.
   * </p>
   *
   * <p>
   *   Note: layout customizations are overlaid on top of the table layout without modifying
   *   the actual layout of the table.
   * </p>
   *
   * <h1> Examples </h1>
   *
   * <h2> Overriding an Avro reader schema </h2>
   *
   * You may override the Avro reader schema used to decode a column with
   * {@link CellSpec#setReaderSchema(org.apache.avro.Schema)}:
   *
   * <pre><tt>{@code
   *   final KijiTable table = ...
   *   final KijiColumnName column = new KijiColumName("family", "qualifier");
   *   // Force the Avro reader schema for family:qualifier to be this schema:
   *   final Schema myReaderSchema = ...
   *
   *   final Map<KijiColumnName, CellSpec> overrides = ImmutableMap.builder()
   *       .put(column, table.getLayout().getCellSpec(column).setReaderSchema(myReaderSchema))
   *       .build();
   *   final KijiTableReader reader = table.getReaderFactory().openTableReader(overrides);
   *   try {
   *      ...
   *   } finally {
   *     reader.close();
   *   }
   * }</tt></pre>
   *
   * <h2> Decoding cells using the Avro writer schemas </h2>
   *
   * You may configured cells of a column to be decoded using the writer schemas using
   * {@link CellSpec#setUseWriterSchema()}:
   *
   * <pre><tt>{@code
   *   final KijiTable table = ...
   *   final KijiColumnName column = new KijiColumName("family", "qualifier");
   *
   *   final Map<KijiColumnName, CellSpec> overrides = ImmutableMap.builder()
   *       .put(column, table.getLayout().getCellSpec(column).setUseWriterSchema())
   *       .build();
   *   final KijiTableReader reader = table.getReaderFactory().openTableReader(overrides);
   *   try {
   *      ...
   *   } finally {
   *     reader.close();
   *   }
   * }</tt></pre>
   *
   * <p> Note:
   *     when a reader is configured to decode a column using the Avro writer schemas of each cell,
   *     each decoded cell may have a different schema. For this reason, we enforce the use of
   *     generic records in this case.
   * </p>
   *
   * <h2> Decoding cells as Avro generic records </h2>
   *
   * You may configure cells from a column to be decoded as Avro generic records with
   * {@link CellSpec#setDecoderFactory(KijiCellDecoderFactory)}:
   *
   * <pre><tt>{@code
   *   final KijiTable table = ...
   *   final KijiColumnName column = new KijiColumName("family", "qualifier");
   *
   *   final Map<KijiColumnName, CellSpec> overrides = ImmutableMap.builder()
   *       .put(column, table.getLayout().getCellSpec(column)
   *           .setDecoderFactory(GenericCellDecoderFactor.get()))
   *       .build();
   *   final KijiTableReader reader = table.getReaderFactory().openTableReader(overrides);
   *   try {
   *     final KijiDataRequest dataRequest = KijiDataRequest.builder()
   *         .addColumns(ColumnsDef.create().add("family", "qualifier"))
   *         .build();
   *     final EntityId entityId = table.getEntityId(...);
   *     final KijiRowData row = reader.get(entityId, dataRequest);
   *     final GenericRecord record = row.getMostRecentValue("family", "qualifier");
   *     ...
   *   } finally {
   *     reader.close();
   *   }
   * }</tt></pre>
   *
   * <p>
   *   This customization will force the use Avro generic records even if appropriate Avro specific
   *   records are available on the classpath.
   * </p>
   *
   * @param overrides Map of column specifications overriding the actual table layout.
   * @return a new KijiTableReader.
   * @throws IOException on I/O error.
   * @deprecated use {@link #readerBuilder}.
   */
  @Deprecated
  KijiTableReader openTableReader(Map<KijiColumnName, CellSpec> overrides) throws IOException;

  /**
   * Get a new KijiTableReaderBuilder which builds readers for the table associated with this
   * factory.
   *
   * <p>
   *   The reader builder provides several options for setting the behavior of a table reader.
   *   <ul>
   *     <li>
   *       OnDecoderCacheMiss allows setting the reader's behavior when no cell decoder is found in
   *       the reader's cache for a particular read request. This may happen if a user specifies a
   *       ColumnReaderSpec as an override in a KijiDataRequest without having specified that
   *       ColumnReaderSpec during construction of the reader. This option defaults to
   *       BUILD_AND_CACHE which causes the reader to build new cell decoders for overrides from
   *       data requests and store them in its cache to serve future requests.
   *     </li>
   *     <li>
   *       ColumnReaderSpec overrides allow setting the default read behavior per column. If an
   *       override exists for a column, data requests which include that column will be fulfilled
   *       according to the specification in the ColumnReaderSpec, even if the data request did not
   *       explicitly request the override. If a data request includes a ColumnReaderSpec override
   *       different from the override specified during construction of the reader, the data
   *       request's override will take precedence. This option defaults to not override any
   *       columns.
   *     </li>
   *     <li>
   *       ColumnReaderSpec alternatives instruct the reader to create cell decoders for all
   *       specified ColumnReaderSpecs, but not to override the default read behavior. Setting these
   *       alternatives can improve performance and will prevent exceptions when using
   *       {@link org.kiji.schema.KijiTableReaderBuilder.OnDecoderCacheMiss#FAIL} if the
   *       ColumnReaderSpec override in a KijiDataRequest was also specified in the alternatives
   *       map during construction of the reader. This option defaults to not include any
   *       alternatives.
   *     </li>
   *   </ul>
   * </p>
   *
   * <p>
   *   Readers returned from this builder must be closed when they are no longer needed to release
   *   underlying resources.
   * </p>
   *
   * @return a new KijiTableReaderBuilder.
   */
  KijiTableReaderBuilder readerBuilder();
}
