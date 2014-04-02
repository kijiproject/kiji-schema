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

package org.kiji.schema.util;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/** In-process implementation of Lock, for testing purposes. */
@ApiAudience.Framework
@ApiStability.Experimental
public final class LocalLock implements Lock {
  private static final Logger LOG = LoggerFactory.getLogger(LocalLock.class);

  private boolean mLocked = false;

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // Nothing to close
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void lock() throws IOException {
    lock(0.0);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean lock(double timeout) throws IOException {
    final long absoluteDeadlineMS =
        (timeout > 0.0) ? System.currentTimeMillis() + (long) (1000 * timeout) : 0;

    while (mLocked) {
      try {
        if (timeout > 0.0) {
          wait((long) (timeout * 1000));
        } else {
          wait();
        }
      } catch (InterruptedException ie) {
        LOG.error(ie.toString());
      }
      if ((absoluteDeadlineMS > 0) && (System.currentTimeMillis() >= absoluteDeadlineMS)) {
        return false;
      }
    }
    mLocked = true;
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void unlock() throws IOException {
    Preconditions.checkState(mLocked);
    mLocked = false;
    notify();
  }
}
