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

/**
 * HBase-specific KijiSchema behavior.
 *
 * <p>Most of KijiSchema implements an abstract key-value store database on top of
 * some underlying physical storage medium. In practice this is built on top of
 * Apache HBase. HBase-specific options and mechanisms that are part of KijiSchema's API
 * reside in this package.
 * </p>
 */
package org.kiji.schema.hbase;
