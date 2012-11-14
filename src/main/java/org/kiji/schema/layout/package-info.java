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
 * Kiji table layouts.
 *
 * <p>
 * A Kiji table has a <i>layout</i> that describes the locality groups, families, and
 * columns it contains.
 *
 * This package contains code for serializing the layout metadata to and from JSON or binary Avro
 * records, and persisting it into the Kiji system tables in HBase.
 *
 * Kiji table layouts are persisted in the {@link org.kiji.schema.KijiMetaTable}.
 * Kiji records the history of the layouts of each table.
 * When updating the layout of a table, Kiji validates the new layout against the current layout
 * of the table.
 *
 * Kiji table layouts are serialized to descriptors as {@link org.kiji.schema.avro.TableLayoutDesc}
 * Avro records (see the Avro record definitions in {@code src/main/avro/Layout.avdl}).
 *
 * Kiji table layouts are internally represented as {@link org.kiji.schema.layout.KijiTableLayout}
 * instances. {@link org.kiji.schema.layout.KijiTableLayout KijiTableLayout} wraps layout
 * descriptors, validate layout updates, assigns column IDs and provides convenience accessors.
 * </p>
 */
package org.kiji.schema.layout;
