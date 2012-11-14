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

package org.kiji.schema.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.kiji.schema.EntityId;

/**
 * A buffer used to hold data entries waiting to be written to a KijiTable.
 *
 * @param <T> The type of the elements being stored in the buffer.
 */
public class KijiDataBuffer<T> {
  /** Stores a list of the internal representation of the wrapped data by EntityId. */
  private Map<EntityId, List<T>> mBuffer;
  /** The current number of elements stored in this buffer. */
  private int mBufferSize;

  /**
   * Creates an instance.
   */
  public KijiDataBuffer() {
    mBuffer = new HashMap<EntityId, List<T>>();
    mBufferSize = 0;
  }

  /**
   * Adds a buffer element to the buffer and associates it with the specified entity.
   *
   * @param id The entity id.
   * @param value The buffer element to add to the buffer.
   * @return A reference to {@code this} to allow chaining of add statements.
   */
  public KijiDataBuffer<T> add(EntityId id, T value) {
    getBuffer(id).add(value);
    mBufferSize++;
    return this;
  }

  /**
   * Clears the buffer by removing all elements stored in it.
   */
  public void clear() {
    mBuffer.clear();
    mBufferSize = 0;
  }

  /**
   * Gets the list of buffer elements associated with the specified entity.
   *
   * @param id The entity id.
   * @return The list of buffer elements used for buffering.
   */
  protected List<T> getBuffer(EntityId id) {
    List<T> returnMe;

    if (mBuffer.containsKey(id)) {
      returnMe = mBuffer.get(id);
    } else {
      returnMe = new ArrayList<T>();
      mBuffer.put(id, returnMe);
    }

    return returnMe;
  }

  /**
   * @return The elements stored in this buffer grouped by their entityId.
   */
  public Collection<List<T>> getBuffers() {
    return mBuffer.values();
  }

  /**
   * @return The {@link java.util.Map.Entry}s that are stored in the buffer.
   */
  public Set<Map.Entry<EntityId, List<T>>> getEntries() {
    return mBuffer.entrySet();
  }

  /**
   * @return The number of elements stored in this buffer.
   */
  public int size() {
    return mBufferSize;
  }
}
