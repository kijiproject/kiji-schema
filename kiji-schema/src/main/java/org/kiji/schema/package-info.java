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

/**
 * The main package for users of KijiSchema.
 *
 * <p>
 * KijiSchema provides layout and schema management on top of HBase.  KijiSchema also allows
 * for the use of both structured and unstructured data in HBase using Avro serialization.
 * </p>
 *
 * <p>
 *   Classes of note:
 * </p>
 * <ul>
 *   <li>{@link org.kiji.schema.Kiji Kiji} - Root class representing a Kiji instance which
 *       provides access to KijiTables.</li>
 *   <li>{@link org.kiji.schema.KijiTable KijiTable} - provides access to
 *       {@link org.kiji.schema.KijiTableReader KijiTableReader} and
 *       {@link org.kiji.schema.KijiTableWriter KijiTableWriter}</li>
 *   <li>{@link org.kiji.schema.KijiTablePool KijiTablePool} - maintains a shared pool of
 *       connections to KijiTables.</li>
 *   <li>{@link org.kiji.schema.KijiDataRequest KijiDataRequest} - a data request for column data
 *       from KijiTables.</li>
 *   <li>{@link org.kiji.schema.KijiRowData KijiRowData} - data for a single entity.</li>
 *   <li>{@link org.kiji.schema.EntityIdFactory EntityIdFactory} - factory for
 *       {@link org.kiji.schema.EntityId}s which identify particular rows in KijiTables. </li>
 * </ul>
 *
 * <p>
 *   Related Documentation:
 * </p>
 * <ul>
 *   <li><a target="_top" href=http;//docs.kiji.org/tutorials>
 *     Tutorials</a></li>
 *   <li><a target="_top" href=http://docs.kiji.org/userguide>
 *     User Guide</a></li>
 * </ul>
 */
package org.kiji.schema;
