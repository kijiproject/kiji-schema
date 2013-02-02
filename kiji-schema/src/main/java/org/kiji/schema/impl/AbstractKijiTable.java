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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.Debug;

/**
 * Main handle for a Kiji table with basic functionality.
 *
 * A KijiTable has all the data about an entity.
 * Entity data is keyed by the entityId.
 * Each row in the table represents one entity.
 */
@ApiAudience.Private
public abstract class AbstractKijiTable implements KijiTable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractKijiTable.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger(AbstractKijiTable.class.getName() + ".Cleanup");

  /** The kiji instance this table belongs to. */
  private final Kiji mKiji;

  /** The name of this table (the Kiji name, not the HBase name). */
  private final String mName;

  /** URI of this table. */
  private final KijiURI mTableURI;

  /** Whether the table is open. */
  private boolean mIsOpen;

  /** String representation of the call stack at the time this object is constructed. */
  private String mConstructorStack;

  /**
   * Creates a new <code>KijiTable</code> instance.
   *
   * @param kiji The kiji instance.
   * @param name The name of the kiji table.
   * @throws IOException on I/O error.
   */
  protected AbstractKijiTable(Kiji kiji, String name) throws IOException {
    mKiji = kiji;
    mName = name;
    mTableURI = KijiURI.newBuilder(mKiji.getURI()).withTableName(mName).build();
    mIsOpen = true;
    if (CLEANUP_LOG.isDebugEnabled()) {
      mConstructorStack = Debug.getStackTrace();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Kiji getKiji() {
    return mKiji;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return mName;
  }

  /** {@inheritDoc} */
  @Override
  public KijiURI getURI() {
    return mTableURI;
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId(String kijiRowKey) {
    return getEntityIdFactory().fromKijiRowKey(kijiRowKey);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (null == obj) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!getClass().equals(obj.getClass())) {
      return false;
    }
    final KijiTable other = (KijiTable) obj;

    // Equal if the two tables have the same URI:
    return mTableURI.equals(other.getURI());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mTableURI.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    if (!mIsOpen) {
      LOG.warn("close() called on an KijiTable that was already closed.");
      return;
    }
    mIsOpen = false;
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      LOG.warn("Closing KijiTable {} in finalize(). You should close it explicitly", mTableURI);
      if (CLEANUP_LOG.isDebugEnabled()) {
        CLEANUP_LOG.debug("Call stack when this KijiTable was constructed:\n{}", mConstructorStack);
      }
      close();
    }
    super.finalize();
  }
}
