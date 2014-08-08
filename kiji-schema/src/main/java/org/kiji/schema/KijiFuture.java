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

package org.kiji.schema;

import com.google.common.util.concurrent.ListenableFuture;

import org.kiji.annotations.ApiAudience;

/**
 * A wrapped ListenableFuture to represent the results of asynchronous calls. Since
 * {@code KijiFuture} extends {@code ListenableFuture}, you can use {@code Futures} with a
 * {@code KijiFuture} to perform transformations and add callbacks.
 * @param <T> value returned by the {@code KijiFuture}.
 */
@ApiAudience.Public
public interface KijiFuture<T> extends ListenableFuture<T> {

}
