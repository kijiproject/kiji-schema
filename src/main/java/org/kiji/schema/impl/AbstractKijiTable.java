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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;

/**
 * Main handle for a Kiji table with basic functionality.
 *
 * A KijiTable has all the data about an entity.
 * Entity data is keyed by the entityId.
 * Each row in the table represents one entity.
 */
public abstract class AbstractKijiTable implements KijiTable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTable.class);

  /** The kiji instance this table belongs to. */
  private final Kiji mKiji;

  /** The name of this table (the Kiji name, not the HBase name). */
  private final String mName;

  /** Whether the table is open. */
  private boolean mIsOpen;

  /** String representation of the call stack at the time this object is constructed. */
  private String mConstructorStack;

  /**
   * Opens a kiji table with the default implementation.
   *
   * @param kiji The kiji instance.
   * @param name The name of the table to open.
   * @return An opened KijiTable.
   * @throws IOException If there is an error.
   */
  public static KijiTable open(Kiji kiji, String name) throws IOException {
    return new HBaseKijiTable(kiji, name);
  }

  /**
   * Creates a new <code>KijiTable</code> instance.
   *
   * @param kiji The kiji instance.
   * @param name The name of the kiji table.
   */
  protected AbstractKijiTable(Kiji kiji, String name) {
    mKiji = kiji;
    mName = name;
    mIsOpen = true;
    if (LOG.isDebugEnabled()) {
      try {
        throw new Exception();
      } catch (Exception e) {
        mConstructorStack = StringUtils.stringifyException(e);
      }
    }
  }

  /**
   * Gets the kiji instance this table lives in.
   *
   * @return The Kiji instance this table belongs to.
   */
  public Kiji getKiji() {
    return mKiji;
  }

  /**
   * Gets the name of this kiji table.
   *
   * @return The name of the table.
   */
  public String getName() {
    return mName;
  }

  /**
   * Creates an entity ID out of a UTF8 encoded Kiji row key.
   *
   * @param kijiRowKey UTF8 encoded Kiji row key.
   * @return an entity ID with the specified Kiji row key.
   */
  public EntityId getEntityId(String kijiRowKey) {
    return getEntityIdFactory().fromKijiRowKey(kijiRowKey);
  }

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

    // TODO Check that those are referring to the same HBase instance.
    // Equal if the kiji instance name and the table name are the same.
    return getKiji().getName().equals(other.getKiji().getName())
        && getName().equals(other.getName());
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(getKiji().getName())
        .append(getName())
        .toHashCode();
  }

  @Override
  public void close() throws IOException {
    if (!mIsOpen) {
      LOG.warn("close() called on an KijiTable that was already closed.");
      return;
    }
    mIsOpen = false;
  }

  @Override
  protected void finalize() throws Throwable {
    if (mIsOpen) {
      LOG.warn("Closing KijiTable " + mName + " in finalize(). You should close it explicitly");
      if (LOG.isDebugEnabled()) {
        LOG.debug("Call stack when this Kiji was constructed: ");
        LOG.debug(mConstructorStack);
      }
      close();
    }
    super.finalize();
  }
}
