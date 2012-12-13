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

package org.kiji.schema.mapreduce;

import org.apache.hadoop.io.Writable;

import org.kiji.annotations.ApiAudience;

/**
 * An output operation from a Kiji map-reduce job. The available operations are:
 * <ul>
 *   <li>{@link KijiDelete}</li>
 *   <li>{@link KijiPut}</li>
 *   <li>{@link KijiIncrement}</li>
 * </ul>
 */
@ApiAudience.Framework
public interface KijiMutation extends Writable { }
