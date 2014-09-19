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

package org.kiji.schema.impl;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.ImmutableList;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiResult;

/**
 * A {@code KijiResult} with no cells.
 *
 * @param <T> The type of {@code KijiCell} values in the view.
 */
@ApiAudience.Private
public class EmptyKijiResult<T> implements KijiResult<T> {

  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;

  /**
   * Constructor for an empty kiji result.
   *
   * @param entityId The entity id of the row to which the kiji result belongs.
   * @param dataRequest The data request defining what columns are requested as part of this result.
   */
  public EmptyKijiResult(final EntityId entityId, final KijiDataRequest dataRequest) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public Iterator<KijiCell<T>> iterator() {
    return ImmutableList.<KijiCell<T>>of().iterator();
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <U extends T> KijiResult<U> narrowView(final KijiColumnName column) {
    final KijiDataRequest narrowRequest = DefaultKijiResult.narrowRequest(column, mDataRequest);
    if (mDataRequest.equals(narrowRequest)) {
      return (KijiResult<U>) this;
    } else {
      return new EmptyKijiResult<U>(mEntityId, narrowRequest);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
  }
}
